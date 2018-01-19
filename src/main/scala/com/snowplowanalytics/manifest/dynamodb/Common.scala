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
import scala.language.higherKinds
import scala.util.control.NonFatal

import java.util.{Map => JMap}

import cats._
import cats.implicits._

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._

import core.ProcessingManifest._
import core.ManifestError
import core.ManifestError._

object Common {

  /** Raw DynamoDB record representation */
  type DbItem = JMap[String, AttributeValue]

  // Primary table
  private[dynamodb] val ItemIdKey = "ItemId"
  private[dynamodb] val AppStateKey = "AppState"
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

  object IO {
    def apply[F[_]: ManifestAction, A](a: => A): F[A] = try {
      a.pure[F]
    } catch {
      case NonFatal(e) =>
        val message = s"DynamoDB Processing Manifest error: ${Option(e.getMessage).getOrElse("no error message")}"
        val error: ManifestError = IoError(message)
        error.raiseError[F, A]
    }
  }

  def scan[F[_]: ManifestAction](client: AmazonDynamoDB, req: Eval[ScanRequest]): F[List[DbItem]] = {
    val scanResult = try {
      val firstResponse = client.scan(req.value)
      val initAcc = firstResponse.getItems.asScala.toList
      goScan(client, firstResponse, initAcc, req).pure[F]
    } catch {
      case NonFatal(e) =>
        val error: ManifestError = IoError(e.getMessage)
        error.raiseError[F, List[DbItem]]
    }

    scanResult
  }

  @tailrec def goScan(client: AmazonDynamoDB,
                      last: ScanResult,
                      acc: List[DbItem],
                      request: Eval[ScanRequest]): List[DbItem] = {
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
                                   (implicit F: MonadError[F, ManifestError]): F[List[DbItem]] = {
    val queryResult = try {
      val firstResponse = client.query(req.value)
      val initAcc = firstResponse.getItems.asScala.toList
      goQuery(client, firstResponse, initAcc, req).pure[F]
    } catch {
      case NonFatal(e) =>
        val error: ManifestError = IoError(e.getMessage)
        error.raiseError[F, List[DbItem]]
    }

    queryResult
  }

  @tailrec private def goQuery(client: AmazonDynamoDB,
                               last: QueryResult,
                               acc: List[DbItem],
                               request: Eval[QueryRequest]): List[DbItem] = {
    Option(last.getLastEvaluatedKey) match {
      case Some(key) =>
        val req = request.value.withExclusiveStartKey(key).withConsistentRead(true)
        val response = client.query(req)
        val items = response.getItems
        goQuery(client, response, items.asScala.toList ++ acc, request)
      case None => acc
    }
  }
}
