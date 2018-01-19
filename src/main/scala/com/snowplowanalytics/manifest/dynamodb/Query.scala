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

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model.{ ScanRequest, ScanResult, AttributeValue }

import com.snowplowanalytics.manifest.core.ProcessingManifest.ManifestAction
import com.snowplowanalytics.manifest.core._
import com.snowplowanalytics.manifest.dynamodb.Common._

import fs2._

import scala.collection.convert.decorateAsJava._
import scala.collection.convert.decorateAsScala._

import Serialization.parseItemId

object Query {
  def fetch[F[_]](client: AmazonDynamoDB, tableName: String)
                 (processedBy: Option[Application], state: Option[State])
                 (implicit F: ManifestAction[F], S: Sync[F]): Stream[F, ItemId] = {
    val records = for {
      request <- Stream.eval(stateRequest(tableName)(processedBy.toList, state))
      item <- scan[F](client, request)
    } yield item

    records.through(parseIds)
  }

  def parseIds[F[_]: ManifestAction]: Pipe[F, DbRecord, ItemId] =
    _.evalMap(parseItemId(_).foldF[F])

  /** Create DynamoDB request, filtering Records with specified `state` and `processedBy` applications */
  def stateRequest[F[_]](tableName: String)
                        (processedBy: List[Application], state: Option[State])
                        (implicit F: Sync[F]): F[ScanRequest] = {
    val original = new ScanRequest()
      .withTableName(tableName)
      .withProjectionExpression(ItemIdKey)
      .withConsistentRead(true)

    val stateParams = state.map(s => (s"$StateKey = :rstate", Map(":rstate" -> s.show)))
    stateParams match {
      case Some((expression, values)) =>           // Will overwritten if applications are set
        original.setFilterExpression(expression)
        original.setExpressionAttributeValues(values.mapValues(new AttributeValue(_)).asJava)
      case None => ()
    }

    val request = buildQueryParameters(processedBy) match {
      case Some((filterExpression, attributeValues)) =>
        val stateExpression = stateParams.map(_._1).map(s => s" AND $s").getOrElse("")
        val stateValues = stateParams.map(_._2).getOrElse(Map.empty)
        original
          .withFilterExpression(filterExpression ++ stateExpression)
          .withExpressionAttributeValues((attributeValues ++ stateValues).mapValues(new AttributeValue(_)).asJava)
      case None => original
    }

    F.delay(request)
  }

  /** Stream of Amazon DynamoDB records */
  def scan[F[_]](client: AmazonDynamoDB, req: ScanRequest)
                (implicit F: MonadError[F, ManifestError], S: Sync[F]): Stream[F, DbRecord] = {

    def send(queryRequest: ScanRequest): F[ScanResult] =
      S.delay(client.scan(queryRequest))

    def go(request: ScanRequest, acc: Stream[F, ScanResult]): Stream[F, ScanResult] = for {
      response <- Stream.eval(send(request))
      result = acc ++ Stream.emit(response).covary[F]
      tail <- Option(response.getLastEvaluatedKey).map(request.withExclusiveStartKey) match {
        case Some(updatedRequest) =>
          go(updatedRequest, result)
        case None => result
      }
    } yield tail

    go(req, Stream.empty).flatMap(response => Stream.emits(response.getItems.asScala).covary[F])
  }

  def buildQueryParameters(applications: List[Application]): Option[(String, Map[String, String])] = {
    applications match {
      case Nil => None
      case single :: Nil => Some(getQueryParams(single, 1))
      case _ =>
        val start = (1, "", Map.empty[String, String])
        val (_, queryResult, valuesResult) = applications.foldLeft(start) {
          case ((i, queryAccum, values), cur) =>
            val (q, v) = getQueryParams(cur, i)
            val query = queryAccum ++ (if (i == 1) s"($q)" else s" OR ($q)")
            (i + 1, query, values ++ v)
        }
        (queryResult, valuesResult).some
    }
  }

  private def getQueryParams(application: Application, index: Int) = {
    val instanceId = s":instanceId$index"
    val appName = s":appName$index"
    val (query, values) = application.instanceId match {
      case Some(id) =>
        (s"$ApplicationInstanceIdKey = $instanceId AND ", Map(instanceId -> id))
      case None => ("", Map.empty)
    }
    (query ++ s"$ApplicationNameKey = $appName", values ++ Map(appName -> application.name))
  }
}
