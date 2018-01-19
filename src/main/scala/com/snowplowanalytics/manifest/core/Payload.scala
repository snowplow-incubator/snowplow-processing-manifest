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
import scala.collection.convert.decorateAsScala._

import cats.implicits._

import io.circe._
import io.circe.syntax._

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._

import com.github.fge.jsonschema.core.report.ProcessingMessage

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ValidatableJsonMethods

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

object Payload {
  val LockedSchemaKey =
    SchemaKey("com.snowplowanalycs.manifest", "locked_payload", "jsonschema", SchemaVer.Full(1, 0, 0))

  val ApplicationErrorSchemaKey =
    SchemaKey("com.snowplowanalycs.snowplow", "application_error", "jsonschema", SchemaVer.Full(1, 0, 2))

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
    json.toData.fold("Not valid self-describing JSON in payload".asLeft[SelfDescribingData[Json]])(_.asRight[String])

  /** Validate Record's payload against its schema */
  private[core] def validate(resolver: Resolver, payload: Payload): Either[String, Unit] = {
    payload.schema.version match {
      case _: SchemaVer.Full =>
        val jsonNodeInstance = circeToJackson(payload.normalize)
        ValidatableJsonMethods.validate(jsonNodeInstance, dataOnly = false)(resolver) match {
          case scalaz.Success(_) =>
            ().asRight
          case scalaz.Failure(messages) =>
            errorFromProcessMessages(messages, payload.schema).asLeft
        }
      case _: SchemaVer.Partial =>
        s"Iglu error for ${payload.schema.toSchemaUri}: processing manifest does not support partial versions".asLeft
    }
  }

  /** Turn Jackson JsonNode into Circe JSON. Does not handle long numbers (> Int.MaxValue) well */
  def jacksonToCirce(jsonNode: JsonNode): Json = {
    jsonNode.getNodeType match {
      case JsonNodeType.ARRAY =>
        val values = jsonNode.elements().asScala.map(jacksonToCirce).toVector
        Json.arr(values: _*)
      case JsonNodeType.BOOLEAN =>
        Json.fromBoolean(jsonNode.asBoolean)
      case JsonNodeType.STRING =>
        Json.fromString(jsonNode.asText)
      case JsonNodeType.NULL =>
        Json.Null
      case JsonNodeType.NUMBER =>
        if (jsonNode.isBigDecimal) {
          Json.fromBigDecimal(jsonNode.decimalValue())
        } else if (jsonNode.isBigInteger) {
          Json.fromBigInt(jsonNode.bigIntegerValue())
        } else if (jsonNode.isDouble) {
          Json.fromDouble(jsonNode.asDouble).getOrElse(Json.Null)
        } else if (jsonNode.isFloat) {
          Json.fromFloat(jsonNode.floatValue).getOrElse(Json.Null)
        } else if (jsonNode.isInt) {
          Json.fromInt(jsonNode.asInt)
        } else {
          Json.fromDouble(jsonNode.asDouble).getOrElse(Json.Null)
        }
      case JsonNodeType.OBJECT =>
        val fields = jsonNode.fields().asScala.map(m => (m.getKey, jacksonToCirce(m.getValue)))
        Json.obj(fields.toList: _*)
      case _ =>
        Json.Null
    }
  }

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

  private def errorFromProcessMessages(scalazNel: scalaz.NonEmptyList[ProcessingMessage],
                                       schemaKey: SchemaKey): String = {
    val errors = scalazNel.list.map(message => jacksonToCirce(message.asJson).noSpaces).mkString(", ")
    s"Iglu error for ${schemaKey.toSchemaUri}: $errors"
  }

  private def stackTraceToString(e: Throwable): Option[String] =
    truncateString(e.getStackTrace.toList.map(_.toString).mkString("\n"), MaxStackLength)

  private def truncateString(s: String, maxLength: Int): Option[String] =
    if (s.isEmpty) None else Some(s.take(maxLength))
}
