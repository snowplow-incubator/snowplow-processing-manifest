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

import scala.annotation.tailrec
import scala.collection.convert.decorateAsScala._
import scala.collection.convert.decorateAsJava._
import scala.language.higherKinds
import scala.util.control.NonFatal

import java.util.{Map => JMap}

import cats._
import cats.implicits._

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._

import core.ProcessingManifest._
import core.ManifestError._
import core.{ Application, State, ManifestError }

object Common {

  /** Raw DynamoDB record representation */
  type DbRecord = JMap[String, AttributeValue]

  // Primary table
  private[dynamodb] val ItemIdKey = "ItemId"
  private[dynamodb] val ApplicationNameKey = "AppName"
  private[dynamodb] val ApplicationVersionKey = "AppVer"
  private[dynamodb] val ApplicationInstanceIdKey = "InstanceId"
  private[dynamodb] val RecordIdKey = "Id"
  private[dynamodb] val StateKey = "RState"   // "State is reserved word in DynamoDB
  private[dynamodb] val TimestampKey = "Timestamp"
  private[dynamodb] val PreviousRecordId = "PreviousRecordId"
  private[dynamodb] val DataKey = "Data"
  private[dynamodb] val AuthorKey = "Author"

  /** Special character used to split string representations of DynamoDB columns into separate entities */
  private[dynamodb] val Separator = ':'

  /** Helper method to turn `Option` into error with default Left value */
  private[dynamodb] def foldO[E, A](option: Option[A], e: => E): Either[E, A] = option match {
    case Some(a) => a.asRight
    case None => e.asLeft
  }

  object ManifestIO {
    def apply[F[_]: ManifestAction, A](a: => A): F[A] = try {
      a.pure[F]
    } catch {
      case NonFatal(e) =>
        val message = s"DynamoDB Processing Manifest error: ${Option(e.getMessage).getOrElse("no error message")}"
        val error: ManifestError = IoError(message)
        error.raiseError[F, A]
    }
  }

  def scan[F[_]: ManifestAction](client: AmazonDynamoDB, req: Eval[ScanRequest]): F[List[DbRecord]] = {
    val scanResult = try {
      val firstResponse = client.scan(req.value)
      val initAcc = firstResponse.getItems.asScala.toList
      goScan(client, firstResponse, initAcc, req).pure[F]
    } catch {
      case NonFatal(e) =>
        val error: ManifestError = IoError(e.getMessage)
        error.raiseError[F, List[DbRecord]]
    }

    scanResult
  }

  @tailrec def goScan(client: AmazonDynamoDB,
                      last: ScanResult,
                      acc: List[DbRecord],
                      request: Eval[ScanRequest]): List[DbRecord] = {
    Option(last.getLastEvaluatedKey) match {
      case Some(key) =>
        val req = request.value.withExclusiveStartKey(key).withConsistentRead(true)
        val response = client.scan(req)
        val items = response.getItems
        goScan(client, response, items.asScala.toList ++ acc, request)
      case None => acc
    }
  }

  private[dynamodb] def query[F[_]](client: AmazonDynamoDB,
                                    req: Eval[QueryRequest])
                                   (implicit F: MonadError[F, ManifestError]): F[List[DbRecord]] = {
    val queryResult = try {
      val firstResponse = client.query(req.value)
      val initAcc = firstResponse.getItems.asScala.toList
      goQuery(client, firstResponse, initAcc, req).pure[F]
    } catch {
      case NonFatal(e) =>
        val error: ManifestError = IoError(e.getMessage)
        error.raiseError[F, List[DbRecord]]
    }

    queryResult
  }

  @tailrec private def goQuery(client: AmazonDynamoDB,
                               last: QueryResult,
                               acc: List[DbRecord],
                               request: Eval[QueryRequest]): List[DbRecord] = {
    Option(last.getLastEvaluatedKey) match {
      case Some(key) =>
        val req = request.value.withExclusiveStartKey(key).withConsistentRead(true)
        val response = client.query(req)
        val items = response.getItems
        goQuery(client, response, items.asScala.toList ++ acc, request)
      case None => acc
    }
  }

  /** Create DynamoDB request to get item ids without PROCESSED:Application records (EXPERIMENTAL) */
  def notProcessedRequest(application: Application): QueryRequest = {
    val original = new QueryRequest().withAttributesToGet(ItemIdKey)

    val attributeValues = Map(
      ":processed" -> new AttributeValue((State.Processed: State).show),
      ":new" -> new AttributeValue((State.New: State).show),
      ":appName" -> new AttributeValue(application.name))

    application.instanceId match {
      case Some(id) =>
        val updated = attributeValues ++ Map(":instanceId" -> new AttributeValue(id))
        original
          .withKeyConditionExpression(s"$StateKey = :processed or $StateKey = :new")
          .withFilterExpression(s"$ApplicationNameKey <> :appName")
          .withFilterExpression(s"$ApplicationInstanceIdKey <> :instanceId")
          .withExpressionAttributeValues(updated.asJava)
      case None =>
        original
          .withKeyConditionExpression(s"$StateKey = :processed or $StateKey = :new")
          .withFilterExpression(s"$ApplicationNameKey <> :appName")
          .withExpressionAttributeValues(attributeValues.asJava)
    }
  }
}
