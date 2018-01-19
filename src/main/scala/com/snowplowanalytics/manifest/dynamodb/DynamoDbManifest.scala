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

import scala.collection.convert.decorateAsJava._
import scala.language.higherKinds
import scala.util.control.NonFatal

import java.time.Instant
import java.util.UUID

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{Record => _, _}
import com.amazonaws.services.dynamodbv2.document.Table

import cats._
import cats.data.NonEmptyList
import cats.implicits._

import com.snowplowanalytics.iglu.client.Resolver

import com.snowplowanalytics.iglu.core.circe.implicits._

import core._
import core.ProcessingManifest._
import core.ManifestError._

import Common._
import Serialization._

/**
  * AWS DynamoDB implementation of processing manifests
  */
class DynamoDbManifest[F[_]](val client: AmazonDynamoDB,
                             val primaryTable: String,
                             val resolver: Resolver)
                            (implicit F: ManifestAction[F])
  extends ProcessingManifest[F](resolver) {
  import DynamoDbManifest._

  /**
    * Turn raw `Record` elements into DynamoDB `DbItem`.
    * Can be overloaded by subclasses
    */
  protected[dynamodb] def newRecord(itemId: ItemId,
                                    app: Application,
                                    recordId: UUID,
                                    previousRecordId: Option[UUID],
                                    stepState: State,
                                    time: Instant,
                                    author: Author,
                                    payload: Option[Payload]): DbItem = {

    val dataMap = payload.map(dataToAttribute)
    val previousRecordMap = previousRecordId.map(uuid => Map(PreviousRecordId -> new AttributeValue().withS(uuid.toString)))
    val item = Map(
      ItemIdKey -> new AttributeValue().withS(itemId),
      AppStateKey -> new AttributeValue().withS(appState(app, recordId, stepState)),
      TimestampKey -> new AttributeValue().withN(time.toEpochMilli.toString),
      AuthorKey -> new AttributeValue().withS(showAuthor(author))
    ) ++ dataMap.getOrElse(Map.empty) ++ previousRecordMap.getOrElse(Map.empty)

    item.asJava
  }

  def list: F[List[Record]] = {
    val req = Eval.always(new ScanRequest().withTableName(primaryTable))
    for {
      dbItems <- scan[F](client, req)
      records <- dbItems.traverse[F, Record](parse(_).foldF)
    } yield records
  }

  def getItem(id: String): F[Option[Item]] = {
    val attributeValues = List(new AttributeValue().withS(id)).asJava
    val condition = new Condition()
      .withComparisonOperator(ComparisonOperator.EQ)
      .withAttributeValueList(attributeValues)
    val req = new QueryRequest(primaryTable)
      .withConsistentRead(true)
      .withKeyConditions(Map(ItemIdKey -> condition).asJava)
    val request = Eval.always(req)

    val response = Common.query[F](client, request)

    for {
      result <- response
      item <- result match {
        case Nil => none[Item].pure[F]
        case h :: t =>
          val records: F[NonEmptyList[Record]] = NonEmptyList(h, t).traverse[F, Record](parse(_).foldF)
          records.flatMap(Item(_).ensure[F](resolver)).map(_.some)
      }
    } yield item
  }

  def put(itemId: ItemId,
          app: Application,
          previousRecordId: Option[UUID],
          state: State,
          author: Option[Agent],
          payload: Option[Payload]): F[(UUID, Instant)] = {

    val time = Instant.now()
    val recordId = UUID.randomUUID()
    val realAuthor = author match {
      case Some(a) => Author(a, Version)
      case None => Author(app.agent, Version)
    }
    val item = newRecord(itemId, app, recordId, previousRecordId, state, time, realAuthor, payload)

    val request = new PutItemRequest()
      .withTableName(primaryTable)
      .withConditionExpression(s"attribute_not_exists($ItemIdKey) AND attribute_not_exists($AppStateKey)")
      .withItem(item)

    try {
      val _ = client.putItem(request)
      (recordId, time).pure[F]
    } catch {
      case _: ConditionalCheckFailedException =>
        parseError(s"Record with id:app:state triple already exists. Cannot write $item").raiseError[F, (UUID, Instant)]
      case NonFatal(e) =>
        val message = s"DynamoDB Run manifest error: ${Option(e.getMessage).getOrElse("no error message")}"
        val error: ManifestError = IoError(message)
        error.raiseError[F, (UUID, Instant)]
    }
  }

  /** Helper method to flatten `Either` (e.g. in case of parse-error) */
  private[manifest] implicit class FoldFOp[A](either: Either[ManifestError, A]) {
    def foldF: F[A] = either.fold(_.raiseError[F, A], _.pure[F])
  }
}

object DynamoDbManifest {

  val DefaultThroughput = 10L

  def apply[F[_]: ManifestAction](dynamodbClient: AmazonDynamoDB,
                                  tableName: String,
                                  resolver: Resolver): DynamoDbManifest[F] =
    new DynamoDbManifest[F](dynamodbClient, tableName, resolver)

  /** Helper method to create manifest-compatible DynamoDB table */
  def create[F[_]: ManifestAction](client: AmazonDynamoDB, tableName: String): F[Unit] = {
    val pks = List(
      new AttributeDefinition(ItemIdKey, ScalarAttributeType.S),
      new AttributeDefinition(AppStateKey, ScalarAttributeType.S))
    val schema = List(
      new KeySchemaElement(ItemIdKey, KeyType.HASH),
      new KeySchemaElement(AppStateKey, KeyType.RANGE))

    val request = new CreateTableRequest()
      .withTableName(tableName)
      .withAttributeDefinitions(pks.asJava)
      .withKeySchema(schema.asJava)
      .withProvisionedThroughput(new ProvisionedThroughput(DefaultThroughput, DefaultThroughput))

    for {
      response <- IO[F, CreateTableResult](client.createTable(request))
      table    <- IO[F, Table](new Table(client, tableName, response.getTableDescription))
      _        <- IO[F, Unit] { table.waitForActive(); () }     // Block until table is available
    } yield ()
  }

  /** Turn `data` (HashMap of JSONs) into DynamoDB-ready Attribute */
  private[dynamodb] def dataToAttribute(payload: Payload): Map[String, AttributeValue] = {
    val attributeValue = new AttributeValue().withS(payload.normalize.noSpaces)
    Map(DataKey -> attributeValue)
  }
}
