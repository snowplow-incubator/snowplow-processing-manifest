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

import java.util.UUID
import java.time.Instant

import io.circe.Json
import io.circe.literal._
import io.circe.syntax._

import cats.implicits._
import cats.data.NonEmptyList

import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.manifest.SpecHelpers
import com.snowplowanalytics.manifest.SpecHelpers._
import com.snowplowanalytics.manifest.pure.PureManifest._

import org.specs2.Specification


class ItemSpec extends Specification { def is = s2"""
  ensure aggregates iglu errors for records $e1
  Find no orphan records in valid Item $e2
  Find orphan records in invalid Item $e3
  Recognize multiple ids in Item as invalid $e4
  Aggregate different type of validation errors $e5
  Item with wildcard cannnot be processed $e6
  """

  def e1 = {

    val payload1 = SelfDescribingData[Json](
      SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1,0,0)),
      json"""{"latitude": 91}""").some

    val payload2 = SelfDescribingData[Json](
      SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1,0,0)),
      json"""{"latitude": 91, "longitude": 1}""").some

    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val id3 = UUID.randomUUID()
    val id4 = UUID.randomUUID()
    val id5 = UUID.randomUUID()

    val time = Instant.now()
    val records = List(   // Still, this is "blocked" Item
      Record("a", Application("app", "0.1.0"), id1, None, State.New, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id2, Some(id1), State.Processing, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id3, Some(id2), State.Failed, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), payload1),
      Record("a", Application("app", "0.1.0"), id4, None, State.Processing, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id5, Some(id4), State.Processed, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), payload2)
    )

    Item(NonEmptyList.fromListUnsafe(records))
      .ensure[PureManifestEffect](igluCentralResolver.runEffect.fold(throw _, identity))
      .runEffect must beLeft.like {
      case ManifestError.Corrupted(ManifestError.Corruption.InvalidContent(errors)) =>
        errors.toList must haveSize(2)
      case _ => ko("Item should have exactly two corrupted errors")
    }
  }

  def e2 = {
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val id3 = UUID.randomUUID()
    val id4 = UUID.randomUUID()
    val id5 = UUID.randomUUID()

    val time = Instant.now()
    val records = List(   // Still, this is "blocked" Item
      Record("a", Application("app", "0.1.0"), id1, None, State.New, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id2, Some(id1), State.Processing, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id3, Some(id2), State.Failed, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id4, None, State.Processing, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id5, Some(id4), State.Processed, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None)
    )

    Item.checkStateConsistency(records) must beRight
  }

  def e3 = {
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val id3 = UUID.randomUUID()
    val id4 = UUID.randomUUID()
    val id5 = UUID.fromString("745f6d57-2b3f-4369-a4c4-9a821730f4ad")
    val id6 = UUID.fromString("37dac157-1b3e-936b-b414-8c821730f4ff")
    val orphanId1 = UUID.fromString("1f274edb-b628-4739-bb5d-5acd40cb91ea")
    val orphanId2 = UUID.fromString("54332edb-1618-4739-bb5d-abcd40cb81fa")

    val time = Instant.now()
    val records = List(
      Record("a", Application("app", "0.1.0"), id1, None, State.New, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id2, Some(id1), State.Processing, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id3, Some(id2), State.Failed, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id4, None, State.Processing, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id5, Some(orphanId1), State.Processed, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app2", "0.1.0"), id6, Some(orphanId2), State.Processing, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None)
    )

    val errors = NonEmptyList.fromListUnsafe(List(
      "Record 745f6d57-2b3f-4369-a4c4-9a821730f4ad with state Processed refers to non-existent record 1f274edb-b628-4739-bb5d-5acd40cb91ea",
      "Record 37dac157-1b3e-936b-b414-8c821730f4ff with state Processing refers to non-existent record 54332edb-1618-4739-bb5d-abcd40cb81fa"))
    Item.checkStateConsistency(records) must beLeft(errors)
  }

  def e4 = {
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val id3 = UUID.randomUUID()
    val id4 = UUID.randomUUID()

    val time = Instant.now()
    val records = List(
      Record("a", Application("app", "0.1.0"), id1, None, State.New, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id2, Some(id1), State.Processing, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("b", Application("app", "0.1.0"), id3, Some(id2), State.Failed, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id4, Some(id3), State.Processing, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None)
    )

    Item.checkItemId(records) must beLeft("Single Item contains multiple ids: a,b")
  }

  def e5 = {
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val orphanId = UUID.fromString("8e6ddfb2-f86a-4916-a3c3-1db65eb66a76")

    val payload = SelfDescribingData[Json](
      SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1,0,0)),
      json"""{"latitude": 91}""")

    val time = Instant.now()
    val records = List(
      Record("a", Application("app", "0.1.0"), id1, None, State.New, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), id2, Some(id1), State.Processing, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), Some(payload)),
      Record("a", Application("app", "0.1.0"), orphanId, None, State.Failed, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None)
    )

    import com.snowplowanalytics.iglu.client.ClientError._
    import com.snowplowanalytics.iglu.client.validator.ValidatorReport
    import com.snowplowanalytics.iglu.client.validator.ValidatorError.InvalidData

    val igluError: ClientError =
      ValidationError(
        InvalidData(
          NonEmptyList.of(
            ValidatorReport("$.longitude: is missing but it is required",Some("$"),List("longitude"),Some("required")),
            ValidatorReport("$.latitude: must have a maximum value of 90.0",Some("$.latitude"),List("90.0"),Some("maximum"))))
      )

    val consistencyError =
      "Record 8e6ddfb2-f86a-4916-a3c3-1db65eb66a76 with state Failed has no previous record"

    Item(NonEmptyList.fromListUnsafe(records))
      .ensure[PureManifestEffect](igluCentralResolver.runEffect.fold(throw _, identity))
      .runEffect must beLeft.like {
      case ManifestError.Corrupted(ManifestError.Corruption.InvalidContent(errors)) =>
        errors.toList must containAllOf(List(igluError.asJson.noSpaces, consistencyError))

    }
  }

  def e6 = {
    def newId = UUID.randomUUID()

    val time = Instant.now()
    val records = List(
      Record("a", Application("app", "0.1.0"), newId, None, State.New, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), newId, None, State.Processing, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("*", "0.1.0"), newId, None, State.Skipped, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), newId, None, State.Failed, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None),
      Record("a", Application("app", "0.1.0"), newId, None, State.Processing, time, Author(Agent("app", "0.1.0"), "0.1.0-rc1"), None)
    )

    val item = SpecHelpers.item(records)

    val used = Item.canBeProcessedBy(Application(Agent("app", "0.2.0"), None))(item) must beFalse
    val unused = Item.canBeProcessedBy(Application(Agent("app2", "0.1.0"), None))(item) must beFalse
    val unusedWithInstance = Item.canBeProcessedBy(Application(Agent("app3", "0.1.0"), Some("foo")))(item) must beFalse
    val usedWithInstance = Item.canBeProcessedBy(Application(Agent("app", "0.1.0"), Some("foo")))(item) must beFalse

    used.and(unused).and(unusedWithInstance).and(usedWithInstance)
  }
}
