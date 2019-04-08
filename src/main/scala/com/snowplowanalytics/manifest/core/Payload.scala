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
package core

import java.util.UUID

import scala.language.higherKinds
import scala.collection.convert.decorateAsJava._

import cats.implicits._

import io.circe._
import io.circe.syntax._

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

object Payload {
  val LockedSchemaKey =
    SchemaKey("com.snowplowanalytics.manifest", "locked_payload", "jsonschema", SchemaVer.Full(1, 0, 0))

  val ApplicationErrorSchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "application_error", "jsonschema", SchemaVer.Full(1, 0, 2))

  // Constants from iglu:com.snowplowanalytics.snowplow/application_error/jsonschema/1-0-2
  private val MaxMessageLength = 2048
  private val MaxStackLength = 8192
  private val MaxExceptionNameLength = 1024

  /** Get payload for failed `Process` */
  def exception(throwable: Throwable): Payload = {
    val stackTrace = stackTraceToString(throwable)
    val causeStackTrace = Option(throwable.getCause).flatMap(stackTraceToString)
    val lineNumber = throwable.getStackTrace.headOption.map(_.getLineNumber)
    val message = truncateString(throwable.getMessage, MaxMessageLength)
      .map(Json.fromString)
      .getOrElse(Json.fromString("No message available"))
    val exceptionName = truncateString(throwable.getClass.getName, MaxExceptionNameLength)
      .map(Json.fromString)
      .getOrElse(Json.Null)

    val data = Map(
      "message" -> message,
      "programmingLanguage" -> Json.fromString("SCALA"),
      "threadId" -> Json.fromLong(Thread.currentThread().getId),
      "stackTrace" -> stackTrace.map(Json.fromString).getOrElse(Json.Null),
      "causeStackTrace" -> causeStackTrace.map(Json.fromString).getOrElse(Json.Null),
      "lineNumber" -> lineNumber.map(Json.fromInt).getOrElse(Json.Null),
      "exceptionName" -> exceptionName)

    SelfDescribingData(ApplicationErrorSchemaKey, JsonObject.fromMap(data).asJson)
  }

  /** Get payload for locked `Item` */
  def locked(lockedBy: UUID): Payload =
    SelfDescribingData(LockedSchemaKey, JsonObject.fromMap(Map("recordId" -> lockedBy.asJson)).asJson)

  def parse(json: Json): Either[String, Payload] =
    SelfDescribingData.parse(json).leftMap(c => s"Not valid self-describing JSON in payload, $c")

  /** Turn Jackson JsonNode into Circe JSON. Does not handle long numbers (> Int.MaxValue) well */
  def circeToJackson(json: Json): JsonNode = {
    json.fold(
      NullNode.instance,
      b => BooleanNode.valueOf(b),
      number => number.toLong match {
        case Some(long) => new LongNode(long)
        case None => number.toBigDecimal match {
          case Some(bigDecimal) => new DecimalNode(bigDecimal.bigDecimal)
          case None => new DoubleNode(number.toDouble)
        }
      },
      string => new TextNode(string),
      array => {
        val factory = JsonNodeFactory.instance
        val javaList = array.map(circeToJackson).toList.asJava
        val node = new ArrayNode(factory)
        node.addAll(javaList)
      },
      jsonObject => {
        val factory = JsonNodeFactory.instance
        val javaMap = jsonObject.toMap.mapValues(circeToJackson).asJava
        new ObjectNode(factory, javaMap)
      }
    )
  }

  private def stackTraceToString(e: Throwable): Option[String] =
    truncateString(e.getStackTrace.toList.map(_.toString).mkString("\n"), MaxStackLength)

  private def truncateString(s: String, maxLength: Int): Option[String] =
    if (s.isEmpty) None else Some(s.take(maxLength))
}
