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
package com.snowplowanalytics.manifest.core

import java.time.Instant
import java.util.UUID

import cats.implicits._

import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._

import org.scalacheck.{Gen, Prop}
import org.specs2.{ScalaCheck, Specification}

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

class RecordSpec extends Specification with ScalaCheck { def is = s2"""
  Correctly encode Record as JSON $e1
  Correctly decode JSON into Record $e2
  Parse any valid JSON Record $e3
  """

  def e1 = {
    val recordId = UUID.fromString("c43f03eb-d481-4c57-a0fa-c2168f4793b9")
    val previousRecordId = UUID.fromString("113f03eb-d481-4c57-a0fa-c2168f4793aa").some
    val time = Instant.ofEpochMilli(1523000478520L)
    val payload = SelfDescribingData(SchemaKey("com.acme", "payload", "jsonschema", SchemaVer.Full(1, 0, 1)),
      parse("{}").getOrElse(throw new RuntimeException("Invalid test JSON")))
    val record = Record("s3://snowplow-data/shredded/run=2018-04-01-23-00-15/",
      Application("rdb-shredder", "0.15.0"),
      recordId,
      previousRecordId,
      State.Processed,
      time,
      Author(Agent("rdb-shredder", "0.15.0"), "0.1.0-rc1"),
      Some(payload)
    )

    val expected = parse(
      """
        |{
        |  "itemId" : "s3://snowplow-data/shredded/run=2018-04-01-23-00-15/",
        |  "application" : {
        |    "agent": {
        |      "name" : "rdb-shredder",
        |      "version" : "0.15.0"
        |    },
        |    "instanceId" : null
        |  },
        |  "recordId" : "c43f03eb-d481-4c57-a0fa-c2168f4793b9",
        |  "previousRecordId": "113f03eb-d481-4c57-a0fa-c2168f4793aa",
        |  "state" : "PROCESSED",
        |  "timestamp" : "2018-04-06T07:41:18.520Z",
        |  "author" : {
        |    "agent": {
        |      "name" : "rdb-shredder",
        |      "version" : "0.15.0"
        |    },
        |    "manifestVersion" : "0.1.0-rc1"
        |  },
        |  "payload" : {
        |    "schema": "iglu:com.acme/payload/jsonschema/1-0-1",
        |    "data": {}
        |  }
        |}
      """.stripMargin).fold(_ => Json.Null, x => x)

    record.asJson must beEqualTo(expected)
  }

  def e2 = {
    val input = parse(
      """
        |{
        |  "itemId" : "s3://snowplow-data/shredded/run=2018-04-01-23-00-15/",
        |  "application" : {
        |    "agent": {
        |      "name" : "rdb-shredder",
        |      "version" : "0.15.0"
        |    },
        |    "instanceId" : null
        |  },
        |  "recordId" : "c43f03eb-d481-4c57-a0fa-c2168f4793b9",
        |  "previousRecordId": null,
        |  "state" : "PROCESSED",
        |  "timestamp" : "2018-04-06T07:41:18.520Z",
        |  "author" : {
        |    "agent": {
        |      "name" : "rdb-shredder",
        |      "version" : "0.15.0"
        |    },
        |    "manifestVersion" : "0.1.0-rc1"
        |  },
        |  "payload" : {
        |    "schema": "iglu:com.acme/payload/jsonschema/1-0-1",
        |    "data": {"some": "key"}
        |  }
        |}
      """.stripMargin).fold(_ => Json.Null, x => x)

    val recordId = UUID.fromString("c43f03eb-d481-4c57-a0fa-c2168f4793b9")
    val payload = SelfDescribingData(SchemaKey("com.acme", "payload", "jsonschema", SchemaVer.Full(1, 0, 1)),
      parse("""{"some": "key"}""").getOrElse(throw new RuntimeException("Invalid test JSON")))
    val time = Instant.ofEpochMilli(1523000478520L)
    val expected = Record("s3://snowplow-data/shredded/run=2018-04-01-23-00-15/",
      Application("rdb-shredder", "0.15.0"),
      recordId,
      None,
      State.Processed,
      time,
      Author(Agent("rdb-shredder", "0.15.0"), "0.1.0-rc1"),
      Some(payload)
    )

    input.as[Record] must beRight(expected)
  }

  def e3 = {
    Prop.forAll(RecordSpec.recordGen) {
      (r: Record) => r.asJson.as[Record] == r.asRight
    }
  }
}

object RecordSpec {

  val stateStepGen = Gen.oneOf(State.ordered)
  val versionGen = Gen.oneOf("0.1.0", "0.2.1", "10.0.1", "0.0.1-rc1", "0.1.0-M3", "1.2.3-rc10")
  val applicationNameGen = Gen.oneOf("rdb-loader", "rdb-shredder", "snowflake-transformer", "data_modeling", "spark-stream-enrich")
  val imposterGen = Gen.oneOf(Some("snowplowctl"), None, Some("other-tool"))
  val instanceIdGen = Gen.frequency((1, Gen.alphaLowerStr.map(_.some)), (10, None))

  val applicationGen = for {
    name <- applicationNameGen
    version <- versionGen
    instanceId <- instanceIdGen
  } yield Application(Agent(name, version), instanceId)

  val idGen = Gen.alphaLowerStr

  val recordGen = for {
    id <- idGen
    application <- applicationGen
    recordId <- Gen.uuid
    prevRecordId <- Gen.option(Gen.uuid)
    step <- stateStepGen
    tstamp <- Gen.calendar.map(_.toInstant)
    imposter <- applicationGen
    authorAgent <- Gen.oneOf(application, imposter).map(_.agent)
    manifestVersion <- versionGen
  } yield Record(id, application, recordId, prevRecordId, step, tstamp, Author(authorAgent, manifestVersion), None)
}
