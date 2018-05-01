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

import java.util.UUID
import java.time.Instant

import scala.collection.convert.decorateAsJava._

import io.circe.Json

import com.amazonaws.services.dynamodbv2.model.AttributeValue

import com.snowplowanalytics.iglu.core.{ SelfDescribingData, SchemaKey }

import org.specs2.Specification

import core._

class SerializationSpec extends Specification { def is = s2"""
  Convert DbRecord to manifest Record $e1
  Convert DbRecord without optional fields to manifest Record $e2
  """

  import SerializationSpec._

  def e1 = {
    val dbRecord = Map(
      "ItemId" -> "s3://some-bucket/snowplow/shredded/run=2018-03-01-04-12-22/".asS,
      "RState" -> "PROCESSED".asS,
      "Id" -> "f1995a27-9894-4827-b72e-54375bcc30ae".asS,
      "InstanceId" -> "44f8b20b-ad1a-4bea-a843-2454d4682cb4".asS,
      "AppName" -> "snowplow-rdb-loader".asS,
      "AppVer" -> "0.15.0".asS,
      "Timestamp" -> 1525450423000L.asN,
      "PreviousRecordId" -> "1b887508-eb44-499c-a768-8418c37a4c46".asS,
      "Author" -> "snowplow-rdb-loader:0.15.0:0.1.0".asS,
      "Data" -> """{"schema": "iglu:com.acme/event/jsonschema/1-0-0", "data": {}}""".asS
    ).asJava

    val expectedPayload = Some(SelfDescribingData[Json](
      SchemaKey.fromUri("iglu:com.acme/event/jsonschema/1-0-0").get,
      Json.fromFields(List.empty))
    )
    val expected = Record(
      "s3://some-bucket/snowplow/shredded/run=2018-03-01-04-12-22/",
      Application(Agent("snowplow-rdb-loader", "0.15.0"), Some("44f8b20b-ad1a-4bea-a843-2454d4682cb4")),
      UUID.fromString("f1995a27-9894-4827-b72e-54375bcc30ae"),
      Some(UUID.fromString("1b887508-eb44-499c-a768-8418c37a4c46")),
      State.Processed,
      Instant.ofEpochMilli(1525450423000L),
      Author(Agent("snowplow-rdb-loader", "0.15.0"), "0.1.0"),
      expectedPayload)

    Serialization.parse(dbRecord) must beRight(expected)
  }

  def e2 = {
    val dbRecord = Map(
      "Id" -> "f1995a27-9894-4827-b72e-54375bcc30ae".asS,
      "ItemId" -> "s3://some-bucket/snowplow/shredded/run=2018-03-01-04-12-22".asS,
      "RState" -> "PROCESSING".asS,
      "AppName" -> "snowplow-rdb-shredder".asS,
      "AppVer" -> "0.14.0".asS,
      "Timestamp" -> 1525450423000L.asN,
      "Author" -> "snowplow-rdb-shredder:0.14.0:0.1.0-rc1".asS
    ).asJava

    val expected = Record(
      "s3://some-bucket/snowplow/shredded/run=2018-03-01-04-12-22",
      Application(Agent("snowplow-rdb-shredder", "0.14.0"), None),
      UUID.fromString("f1995a27-9894-4827-b72e-54375bcc30ae"),
      None,
      State.Processing,
      Instant.ofEpochMilli(1525450423000L),
      Author(Agent("snowplow-rdb-shredder", "0.14.0"), "0.1.0-rc1"),
      None)

    Serialization.parse(dbRecord) must beRight(expected)
  }
}

object SerializationSpec {
  implicit class DynamoDBStringSyntax(s: String) {
    def asS: AttributeValue = new AttributeValue().withS(s)
  }

  implicit class DynamoDBNumberSyntax(i: Long) {
    def asN: AttributeValue = new AttributeValue().withN(i.toString)
  }
}
