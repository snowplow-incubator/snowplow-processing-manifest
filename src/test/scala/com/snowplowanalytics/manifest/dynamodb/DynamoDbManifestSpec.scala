/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.manifest
package dynamodb

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

import java.util.UUID

import cats.implicits._

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder

import org.specs2.Specification
import org.specs2.execute.{AsResult, Result}
import org.specs2.specification.AroundEach

import com.snowplowanalytics.manifest.core._

/** Integration test suite for DynamoDb */
class DynamoDbManifestSpec extends Specification with AroundEach { def is = s2"""
  Put 2001 records $e2
  Fail to acquire lock when another app is processing $e4
  """

  import DynamoDbManifestSpec._

  // Without fake static credentials, provider looks for existing ones
  private val FakeCreds = new AWSStaticCredentialsProvider(new BasicAWSCredentials("fakeaccesskey", "fakesecretkey"))
  private val Config = new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2")
  private val Client = AmazonDynamoDBClientBuilder
    .standard()
    .withCredentials(FakeCreds)
    .withEndpointConfiguration(Config)
    .build()

  val TableName = "test-table"

  /** Create and delete table after each execution */
  def around[R: AsResult](r: => R): Result = {
    DynamoDbManifest.create(Client, TableName)
    try AsResult(r)
    finally DynamoDbManifestSpec.delete(Client, TableName)
  }

  def e2 = {
    val states = 3
    val cardinality = 2001      // 0.to(2000)
    val expectedItems = 667     // 2001 items will be grouped per 3 states

    val records = 0.to(2000).map { i =>
      DynamoDbManifestSpec.RecordPayload(
        s"s3://folder-${i / states}/",
        Application("test", "0.1.0"),
        DynamoDbManifestSpec.intToState(i),
        None)
    }
    val client = new DynamoDbManifest(Client, TableName, SpecHelpers.igluCentralResolver)
    val putResult = records.map(record => client.put(record.id, record.app, None, record.stepState, None, record.payload)).toList.sequence
    val items = client.items

    val retrievedExpectation = items.map(_.keySet.size) must beRight(expectedItems)
    val addedExpectation = putResult.map(_.length) must beRight(cardinality)

    // Get 10 random items just being saved
    // Each item must contain exactly 3 records
    // Otherwise `getItem` does not fetch all corresponding records
    val randomIds = scala.util.Random.shuffle(records).take(10).map(_.id)
    val randomItems = randomIds.foldLeft(List.empty[Option[Item]]) {
      (acc, cur) => client.getItem(cur).fold(x => throw new RuntimeException(x.toString), x => x) :: acc
    }.flatten
    val itemSizesExpectation = randomItems.map(_.records.size) must like {
      case sizes => sizes must contain(allOf(be_==(states)))
    }

    addedExpectation and retrievedExpectation and itemSizesExpectation
  }

  // This spec often fails due sequential (not parallel) Future-execution
  def e3 = {
    def process(item: Item): Try[Option[Payload]] = Try(None)

    val client = new DynamoDbManifest(Client, TableName, SpecHelpers.igluCentralResolver)
    client.put("s3://folder", Application("manifest-test", "0.1.0-rc1"), None, State.New, None, None)

    // Two threads simultaneously set Processing for an item,
    // then realize (on secondary check) that they both did it
    // Both fail without proceeding to actual processing
    var firstAppResult: Try[IO[Unit]] = null
    var secondAppResult: Try[IO[Unit]] = null
    Future {
      client.processAll(Application("app-1", "0.1.0-rc1"), all, None, process)
    } onComplete { case result => firstAppResult = result }
    Future {
      client.processAll(Application("app-2", "0.1.0-rc1"), all, None, process)
    } onComplete { case result => secondAppResult = result }
    Thread.sleep(2000)

    val manifestContent = client.items

    val twoFailuresExpectation = firstAppResult must beSuccessfulTry.like {
      case Left(l1: ManifestError.Locked) =>
        secondAppResult must beSuccessfulTry.like {
          case Left(l2: ManifestError.Locked) if l1 != l2 => ok
          case other => ko(s"Unexpected result from second application: $other. First application's result: $l1. Manifest content: $manifestContent")
        }
      // Futures sometimes fail to start simultaneously and app-1 processes Item without interruption
      case other => ko(s"Unexpected result from first application: $other. Manifest content: $manifestContent")
    }

    val expectedSteps = List(State.New, State.Processing, State.Processing, State.Failed, State.Failed)
    val itemsExpectation = manifestContent must beRight.like {
      case globalState =>
        val singleKey = globalState.keySet must beEqualTo(Set("s3://folder"))
        val values = globalState.values.map(_.records.map(_.state).toList).toList.flatten must beEqualTo(expectedSteps)
        singleKey and values
    }

    twoFailuresExpectation and itemsExpectation
  }

  def e4 = {
    def process(delayMs: Long)(item: Item): Try[Option[Payload]] = {
      Thread.sleep(delayMs)
      Try(None)
    }

    val client = new DynamoDbManifest(Client, TableName, SpecHelpers.igluCentralResolver)
    client.put("s3://folder", Application("manifest-test", "0.1.0-rc1"), None, State.New, None, None)

    // Second thread tries to acquire lock when first already acquired (process is blocking),
    // bot not released yet. First proceeds without problems,
    // second one exits with locked state, without adding anything to manifest
    var firstAppResult: Try[IO[Unit]] = null
    var secondAppResult: Try[IO[Unit]] = null
    Future {
      client.processAll(Application("manifest-test", "0.1.0-rc1"), all, None, process(2000L))
    } onComplete { case result => firstAppResult = result }
    Future {
      Thread.sleep(1000)
      client.processAll(Application("manifest-test", "0.1.0-rc1"), all, None, process(0L))
    } onComplete { case result => secondAppResult = result }
    Thread.sleep(3000)

    val expectedSteps = List(State.New, State.Processing, State.Processed)
    val firstAppExpectation = firstAppResult must beSuccessfulTry.like { case Right(_) => ok }
    val secondAppExpectation = secondAppResult must beSuccessfulTry.like { case Left(_: ManifestError.Locked) => ok }
    val itemsExpectation = client.items must beRight.like {
      case globalState =>
        val singleKey = globalState.keySet must beEqualTo(Set("s3://folder"))
        val values = globalState.values.map(_.records.map(_.state).toList).toList.flatten must beEqualTo(expectedSteps)
        singleKey.and(values)
    }

    firstAppExpectation and secondAppExpectation and itemsExpectation
  }
}

object DynamoDbManifestSpec {
  type IO[A] = Either[ManifestError, A]

  case class RecordPayload(id: String, app: Application, stepState: State, payload: Option[Payload])

  def fromRecord(record: Record): RecordPayload =
    RecordPayload(record.itemId, record.application, record.state, record.payload)

  // Only these three states can be orphaned (without previous record id)
  def intToState(i: Int): State = {
    i % 3 match {
      case 0 => State.New
      case 1 => State.Processing
      case 2 => State.Skipped
    }
  }

  def delete(client: AmazonDynamoDB, tableName: String): Unit = client.deleteTable(tableName)

  def all[A](a: A): Boolean = true
}
