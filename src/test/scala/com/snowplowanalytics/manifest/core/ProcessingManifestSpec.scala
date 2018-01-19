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

import scala.util.{ Try, Failure }
import scala.util.Random.shuffle

import java.time.Instant
import java.util.UUID

import cats.implicits._
import cats.data.NonEmptyList

import io.circe.parser.parse

import com.snowplowanalytics.iglu.core.{ SelfDescribingData, SchemaVer, SchemaKey }

import org.specs2.Specification

import ProcessingManifest._

class ProcessingManifestSpec extends Specification { def is = s2"""
  Correctly identify unprocessed correct Item $e1
  Correctly identify locked Item $e2
  processNewItem adds three correct records $e3
  Correctly filter out items by predicate $e4
  processedBy takes args into account $e5
  processedNewItem with existing NEW adds correct records $e6
  processedNewItem adds correct payload for failed process $e7
  """

  def e1 = {
    val time = Instant.now()
    val id1 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00000")
    val id2 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00001")

    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")
    val records = List(
      Record("1", Application(agent, None), id1, None, State.New, time, author, None),
      Record("1", Application(agent, None), id2, id1.some, State.Processing, time.plusSeconds(10), author, None),
      Record("1", Application(agent, None), UUID.randomUUID(), id2.some, State.Processed, time.plusSeconds(20), author, None)
    )

    val manifest = ProcessingManifestSpec.StaticManifest(records)

    val newAppExpectation = manifest.unprocessed(Application("rdb-loader", "0.13.0"), _ => true) must beRight(List(Item(NonEmptyList.fromListUnsafe(records))))
    val oldAppExpectation = manifest.unprocessed(Application("rdb-shredder", "0.14.0"), _ => true) must beRight(Nil)
    newAppExpectation.and(oldAppExpectation)
  }

  def e2 = {
    val time = Instant.now()
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")
    val id1 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00000")
    val id2 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00001")

    val records = List(
      Record("1", Application(agent, None), id1, None, State.New, time, author, None),
      Record("1", Application(agent, None), id2, id1.some, State.Processing, time.plusSeconds(10), author, None)
    )

    val manifest = ProcessingManifestSpec.StaticManifest(records)

    val newAppExpectation = manifest.unprocessed(Application("rdb-loader", "0.13.0"), _ => true) must beLeft.like {
      case _: ManifestError.Locked => ok
    }
    val oldAppExpectation = manifest.unprocessed(Application("rdb-shredder", "0.14.0"), _ => true) must beLeft.like {
      case _: ManifestError.Locked => ok
    }

    newAppExpectation and oldAppExpectation
  }

  def e4 = {
    val processed = collection.mutable.ListBuffer.newBuilder[Item]
    def process(item: Item) = {
      processed += item
      Try(None)
    }

    val time = Instant.now().minusSeconds(1000L)
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")
    val id1 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00006")
    val id2 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00007")

    val records = List(
      Record("1", Application("rdb-shredder", "0.13.0"), UUID.randomUUID(), None, State.New, time, author, None),

      Record("2", Application("rdb-shredder", "0.13.0"), UUID.randomUUID(), None, State.New, time.plusSeconds(20), author, None),

      Record("3", Application("rdb-shredder", "0.13.0"), id1, None, State.New, time.plusSeconds(70), author, None),
      Record("3", Application("rdb-shredder", "0.13.0"), id2, id1.some, State.Processing, time.plusSeconds(80), author, None),
      Record("3", Application("rdb-shredder", "0.13.0"), UUID.randomUUID(), id2.some, State.Processed, time.plusSeconds(90), author, None)
    )

    val manifest = ProcessingManifestSpec.StaticManifest(records)

    val processingResult = manifest.processAll(
      Application("test-process-function", "0.1.0"),
      Item.processedBy(Application("rdb-shredder", ""), _),
      None,
      process)

    val contentIsCorrect = manifest.items.map(_.values.toList.flatMap(_.records.toList)).toOption.get.map { r =>
      (r.itemId, r.state, r.payload, r.application.name)
    } must containTheSameElementsAs(List(
      ("2", State.New, None, "rdb-shredder"),
      ("1", State.New, None, "rdb-shredder"),
      ("3", State.New, None, "rdb-shredder"),
      ("3", State.Processing, None, "rdb-shredder"),
      ("3", State.Processed, None, "rdb-shredder"),
      ("3", State.Processing, None, "test-process-function"),
      ("3", State.Processed, None, "test-process-function")
    ))
    val processedSingleItem = processed.result().toList.length must beEqualTo(1)

    (processingResult must beRight) and contentIsCorrect and processedSingleItem
  }

  def e5 = {
    val time = Instant.now()
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")

    def newId = UUID.randomUUID()
    val id1 = UUID.randomUUID()

    val item = Item(NonEmptyList(
      Record("a", Application("discoverer", "0.1.0"), UUID.randomUUID(), None, State.New, time, author, None),
      List(
        Record("a", Application("transformer", "0.1.0"), id1, None, State.Processing, time, author, None),
        Record("a", Application("transformer", "0.1.0"), newId, id1.some, State.Processed, time, author, None),

        Record("a", Application(Agent("loader", "0.1.0"), Some("target-1")), newId, None, State.Processed, time, author, None)
      )
    ))

    val transformerProcessed = Item.processedBy(Application("transformer", "0.1.0"), item) must beTrue or
      ko("transformer without instance wasn't marked as processed")
    val transformerUnprocessed = Item.processedBy(Application(Agent("transformer", "0.1.0"), Some("args")), item) must beTrue or
      ko("transformer with instance wasn't marked as processed; adding records with instance is subject to double-processing")
    val loaderProcessed = Item.processedBy(Application(Agent("transformer", "0.1.0"), Some("target-1")), item) must beTrue or
      ko("loader with instance wasn't marked as processed; instance was not taken into account")
    val loaderUnprocessed = Item.processedBy(Application(Agent("loader", "0.1.0"), Some("target-2")), item) must beFalse or
      ko("loader with new instances was marked as processed")

    transformerProcessed and transformerUnprocessed and loaderProcessed and loaderUnprocessed
  }

  def e3 = {
    val app = Application("discoverer", "0.1.0")
    val author = Author(app.agent, "0.1.0-M3")
    val data = parse("""{"message": "no-exception", "programmingLanguage": "SCALA"}""").toOption.get
    val payload = SelfDescribingData(
      SchemaKey("com.snowplowanalytics.snowplow", "application_error", "jsonschema", SchemaVer.Full(1,0,2)),
      data)
    val processedData = parse("""{"message": "success", "programmingLanguage": "SCALA"}""").toOption.get
    val processedPayload = SelfDescribingData(
      SchemaKey("com.snowplowanalytics.snowplow", "application_error", "jsonschema", SchemaVer.Full(1,0,2)),
      processedData)
    val process = () => { Try { Some(processedPayload) } }

    val manifest = for {
      _ <- PureManifest.processNewItem("a", app, Some(payload), process)
    } yield ()

    val (records, result) = manifest.value.run(List.empty).value

    val expectedRecords = List(
      Record("a",app, PureManifest.id(0), None, State.New,PureManifest.StartTime, author, None),
      Record("a",app, PureManifest.id(1), None, State.Processing, PureManifest.StartTime.plusSeconds(1), author, Some(payload)),
      Record("a",app, PureManifest.id(2), Some(PureManifest.id(1)), State.Processed,PureManifest.StartTime.plusSeconds(2),author,Some(processedPayload))
    )

    val success = result must beRight
    val correctRecords = records must containTheSameElementsAs(expectedRecords)

    success and correctRecords
  }

  def e6 = {
    val app = Application("discoverer", "0.1.0")
    val author = Author(app.agent, "0.1.0-M3")
    val data = parse("""{"message": "no-exception", "programmingLanguage": "SCALA"}""").toOption.get
    val payload = SelfDescribingData(
      SchemaKey("com.snowplowanalytics.snowplow", "application_error", "jsonschema", SchemaVer.Full(1,0,2)),
      data)
    val processedData = parse("""{"message": "success", "programmingLanguage": "SCALA"}""").toOption.get
    val processedPayload = SelfDescribingData(
      SchemaKey("com.snowplowanalytics.snowplow", "application_error", "jsonschema", SchemaVer.Full(1,0,2)),
      processedData)
    val process = () => { Try { Some(processedPayload) } }

    val newRecord = Record("a",app, PureManifest.id(0), None, State.New,PureManifest.StartTime, author, None)
    val manifest = for {
      _ <- PureManifest.processNewItem("a", app, Some(payload), process)
    } yield ()

    val (records, result) = manifest.value.run(List(newRecord)).value

    val expectedRecords = List(
      newRecord,
      Record("a",app, PureManifest.id(1), None, State.Processing, PureManifest.StartTime.plusSeconds(1), author, Some(payload)),
      Record("a",app, PureManifest.id(2), Some(PureManifest.id(1)), State.Processed,PureManifest.StartTime.plusSeconds(2),author,Some(processedPayload))
    )

    val success = result must beRight
    val correctRecords = records must containTheSameElementsAs(expectedRecords)

    success and correctRecords
  }

  def e7 = {
    val app = Application("discoverer", "0.1.0")
    val author = Author(app.agent, "0.1.0-M3")
    val data = parse("""{"message": "no-exception", "programmingLanguage": "SCALA"}""").toOption.get
    val payload = SelfDescribingData(
      SchemaKey("com.snowplowanalytics.snowplow", "application_error", "jsonschema", SchemaVer.Full(1,0,2)),
      data)
    val exception = new RuntimeException("BOOM!")
    val process = () => { Try { throw exception } }

    val manifest = for {
      _ <- PureManifest.processNewItem("a", app, Some(payload), process)
    } yield ()

    val (records, result) = manifest.value.run(Nil).value

    val expectedRecords = List(
      Record("a",app, PureManifest.id(0), None, State.New,PureManifest.StartTime, author, None),
      Record("a",app, PureManifest.id(1), None, State.Processing, PureManifest.StartTime.plusSeconds(1), author, Some(payload))
    )

    val failedRecord = records.find(_.state == State.Failed)

    val error = ManifestError.ApplicationError(exception, app, UUID.fromString("55a54008-ad1b-3589-aa21-0d2629c1df41"))

    val failureExpectation = result must beLeft(error)
    val correctRecords = records must containAllOf(expectedRecords)
    val failedRecordExpectation = failedRecord must beSome.like {
      case record =>
        val previous = record.previousRecordId must beSome(UUID.fromString("55a54008-ad1b-3589-aa21-0d2629c1df41"))
        val payload = record.payload must beSome.like {
          case p => p.schema must beEqualTo(SchemaKey("com.snowplowanalytics.snowplow", "application_error", "jsonschema", SchemaVer.Full(1,0,2)))
          case p => ko(s"Unexpected FAILED payload SchemaKey ${p.schema}")
        }
        previous and payload
      case other => ko(s"FAILED record has unexpected structure $other")
    }

    failureExpectation and correctRecords and failedRecordExpectation
  }
}

object ProcessingManifestSpec {

  type F[A] = Either[ManifestError, A]

  case class StaticManifest(records: List[Record]) extends ProcessingManifest[F](SpecHelpers.igluCentralResolver) {

    val stateBuffer = collection.mutable.ListBuffer(records: _*)

    def mixed: List[Record] = shuffle(stateBuffer.toList)

    def getItem(id: ItemId): Either[ManifestError, Option[Item]] = {
      val map = mixed.groupBy(_.itemId).map { case (i, r) => (i, Item(NonEmptyList.fromListUnsafe(r))) }
      Right(map.get(id))
    }

    def put(id: ItemId, app: Application, previousRecordId: Option[UUID], step: State, author: Option[Agent], payload: Option[Payload]): Either[ManifestError, (UUID, Instant)] =
      Right {
        val recordId = UUID.randomUUID()
        val time = Instant.now()
        stateBuffer += Record(id, app, recordId, previousRecordId, step, time, Author(Agent("rdb-shredder", "0.13.0"), "0.1.0"), payload)
        (recordId, time)
      }

    def list: Either[ManifestError, List[Record]] = Right(mixed)
  }

  def all[A](a: A): Boolean = true
}
