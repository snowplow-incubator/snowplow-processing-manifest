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

import scala.util.Try

import java.time.Instant
import java.util.UUID

import cats.implicits._
import cats.data.NonEmptyList

import _root_.io.circe.parser.parse

import com.snowplowanalytics.iglu.core.{ SelfDescribingData, SchemaVer, SchemaKey }

import org.specs2.Specification

import SpecHelpers.{ AssetVersion, TestManifest }
import SpecHelpers.TestManifest._

class ProcessingManifestSpec extends Specification { def is = s2"""
  getUnprocessed identifies unprocessed Item $e1
  getUnprocessed identifies locked Item $e2
  getUnprocessed skips Item filtered out by preprocessor $e8
  processAll filters out items by predicate $e4
  processedBy takes args into account $e5
  processNewItem adds three correct records $e3
  processNewItem with existing NEW adds correct records $e6
  processNewItem adds correct payload for failed process $e7
  query respects Skipped records $e9
  query respects Skipped records with instanceId $e10
  query can be used to query only by processedBy $e11
  query can be used to query only by requestedBy $e12
  getUnprocessed identifies failed Item without preparedBy criterion $e13
  processNewItem does NOT identify failed Item $e14
  getUnprocessed identifies failed Item with preparedBy criterion $e15
  """

  def e1 = {
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")
    val records = List(
      Record("1", Application(agent, None), id(0), None, State.New, time(0), author, None),
      Record("1", Application(agent, None), id(1), id(0).some, State.Processing, time(1), author, None),
      Record("1", Application(agent, None), id(2), id(1).some, State.Processed, time(2), author, None))

    val idsForLoader = TestManifest.query(None, Application("rdb-loader", "0.14.0").some)
    val idsForShredder = TestManifest.query(None, Application("rdb-shredder", "0.13.0").some)

    val newAppExpectation = TestManifest
      .getUnprocessed(idsForLoader, _ => true)
      .run(records) must beRight(List(Item(NonEmptyList.fromListUnsafe(records))))
    val oldAppExpectation = TestManifest
      .getUnprocessed(idsForShredder, _ => true)
      .run(records) must beRight(Nil)

    newAppExpectation and oldAppExpectation
  }

  def e2 = {
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")

    val records = List(
      Record("1", Application(agent, None), id(0), None, State.New, time(0), author, None),
      Record("1", Application(agent, None), id(1), id(0).some, State.Processing, time(1), author, None))
    val lockingRecord = NonEmptyList(records.get(1).get, Nil)

    val idsForLoader = TestManifest.query(None, Application("rdb-loader", "0.13.0").some)
    val idsForShredder = TestManifest.query(None, Application("rdb-loader", "0.14.0").some)

    val newAppExpectation = TestManifest
      .getUnprocessed(idsForLoader, _ => true)
      .run(records) must beLeft(ManifestError.Locked(lockingRecord, None))
    val oldAppExpectation = TestManifest
      .getUnprocessed(idsForShredder, _ => true)
      .run(records) must beLeft(ManifestError.Locked(lockingRecord, None))

    newAppExpectation and oldAppExpectation
  }

  def e3 = {
    val app = Application("discoverer", "0.1.0")
    val author = Author(app.agent, AssetVersion)
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
      _ <- TestManifest.processNewItem("a", app, Some(payload), process)
    } yield ()

    val (records, result) = manifest.value.run(List.empty).unsafeRunSync().toOption.get

    val expectedRecords = List(
      Record("a",app, TestManifest.id(0), None, State.New,TestManifest.StartTime, author, None),
      Record("a",app, TestManifest.id(1), None, State.Processing, TestManifest.StartTime.plusSeconds(1), author, Some(payload)),
      Record("a",app, TestManifest.id(2), Some(TestManifest.id(1)), State.Processed,TestManifest.StartTime.plusSeconds(2),author,Some(processedPayload))
    )

    val success = result must beRight
    val correctRecords = records must containTheSameElementsAs(expectedRecords)

    success and correctRecords
  }

  def e4 = {
    def process(item: Item) = Try(None)

    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")

    val records = List(
      Record("1", Application("rdb-shredder", "0.13.0"), id(0), None, State.New, time(0), author, None),

      Record("2", Application("rdb-shredder", "0.13.0"), id(1), None, State.New, time(1), author, None),

      Record("3", Application("rdb-shredder", "0.13.0"), id(2), None, State.New, time(2), author, None),
      Record("3", Application("rdb-shredder", "0.13.0"), id(3), id(2).some, State.Processing, time(3), author, None),
      Record("3", Application("rdb-shredder", "0.13.0"), id(4), id(3).some, State.Processed, time(4), author, None))

    val newApp = Application("test-process-function", "0.1.0")
    val newAuthor = Author(newApp.agent, AssetVersion)
    val newRecords = List(
      Record("3", newApp, id(5), None, State.Processing, time(5), newAuthor, None),
      Record("3", newApp, id(6), id(5).some, State.Processed, time(6), newAuthor, None))

    val result = for {
      _ <- TestManifest.processAll(Application("test-process-function", "0.1.0"), Item.processedBy(Application("rdb-shredder", ""), _), None, process)
      records <- TestManifest.items.map(_.values.flatMap(_.records.toList).toList)
    } yield records

    result.run(records) must beRight {
      containTheSameElementsAs(records ++ newRecords)
    }
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

  def e6 = {
    val app = Application("discoverer", "0.1.0")
    val author = Author(app.agent, AssetVersion)
    val data = parse("""{"message": "no-exception", "programmingLanguage": "SCALA"}""").toOption.get
    val payload = SelfDescribingData(
      SchemaKey("com.snowplowanalytics.snowplow", "application_error", "jsonschema", SchemaVer.Full(1,0,2)),
      data)
    val processedData = parse("""{"message": "success", "programmingLanguage": "SCALA"}""").toOption.get
    val processedPayload = SelfDescribingData(
      SchemaKey("com.snowplowanalytics.snowplow", "application_error", "jsonschema", SchemaVer.Full(1,0,2)),
      processedData)
    val process = () => { Try { Some(processedPayload) } }

    val newRecord = Record("a",app, TestManifest.id(0), None, State.New,TestManifest.StartTime, author, None)
    val manifest = for {
      _ <- TestManifest.processNewItem("a", app, Some(payload), process)
    } yield ()

    val (records, result) = manifest.value.run(List(newRecord)).unsafeRunSync().toOption.get

    val expectedRecords = List(
      newRecord,
      Record("a",app, TestManifest.id(1), None, State.Processing, TestManifest.StartTime.plusSeconds(1), author, Some(payload)),
      Record("a",app, TestManifest.id(2), Some(TestManifest.id(1)), State.Processed,TestManifest.StartTime.plusSeconds(2),author,Some(processedPayload))
    )

    val success = result must beRight
    val correctRecords = records must containTheSameElementsAs(expectedRecords)

    success and correctRecords
  }

  def e7 = {
    val app = Application("discoverer", "0.1.0")
    val author = Author(app.agent, AssetVersion)
    val data = parse("""{"message": "no-exception", "programmingLanguage": "SCALA"}""").toOption.get
    val payload = SelfDescribingData(
      SchemaKey("com.snowplowanalytics.snowplow", "application_error", "jsonschema", SchemaVer.Full(1,0,2)),
      data)
    val exception = new RuntimeException("BOOM!")
    val process = () => { Try { throw exception } }

    val manifest = for {
      _ <- TestManifest.processNewItem("a", app, Some(payload), process)
    } yield ()

    val (records, result) = manifest.value.run(Nil).unsafeRunSync().toOption.get

    val expectedRecords = List(
      Record("a",app, TestManifest.id(0), None, State.New,TestManifest.StartTime, author, None),
      Record("a",app, TestManifest.id(1), None, State.Processing, TestManifest.StartTime.plusSeconds(1), author, Some(payload))
    )

    val failedRecord = records.find(_.state == State.Failed)

    val error = ManifestError.ApplicationError(exception, app, UUID.fromString("55a54008-ad1b-3589-aa21-0d2629c1df41"))

    val failureExpectation = result must beLeft(error)
    val correctRecords = records must containAllOf(expectedRecords)
    val failedRecordExpectation = failedRecord must beSome.like {
      case Record(_, _, _, previousRecordId, _, _, _, payload) =>
        val previous = previousRecordId must beSome(UUID.fromString("55a54008-ad1b-3589-aa21-0d2629c1df41"))
        val schema = payload.map(_.schema) must beSome(SchemaKey("com.snowplowanalytics.snowplow", "application_error", "jsonschema", SchemaVer.Full(1,0,2)))
        previous and schema
    }

    failureExpectation and correctRecords and failedRecordExpectation
  }

  def e8 = {
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")

    val records = List(
      Record("1", Application(agent, None), id(0), None, State.New, time(0), author, None),
      Record("1", Application(agent, None), id(1), id(0).some, State.Processing, time(1), author, None))

    val ids = TestManifest.query(Application("rdb-shredder", "0.13.0").some, Application("rdb-loader", "0.13.0").some)

    TestManifest
      .getUnprocessed(ids, _ => true)
      .run(records) must beRight(List.empty)
  }

  def e9 = {
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")
    val loader = Application("rdb-loader", "0.14.0")

    val records = List(
      Record("1", Application(agent, None), id(0), None, State.New, time(0), author, None),
      Record("1", Application(agent, None), id(1), id(0).some, State.Processing, time(1), author, None),
      Record("1", Application(agent, None), id(2), id(1).some, State.Processed, time(2), author, None),
      Record("1", loader, id(3), id(2).some, State.Skipped, time(3), author, None))

    TestManifest
      .query(Application("rdb-shredder", "0.13.0").some, Application("rdb-loader", "0.13.0").some)
      .compile.toList
      .run(records) must beRight(List.empty)
  }

  def e10 = {
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")
    val loader = Application(Agent("rdb-loader", "0.14.0"), Some("id1"))

    val records = List(
      Record("1", Application(agent, None), id(0), None, State.New, time(0), author, None),
      Record("1", Application(agent, None), id(1), id(0).some, State.Processing, time(1), author, None),
      Record("1", Application(agent, None), id(2), id(1).some, State.Processed, time(2), author, None),
      Record("1", loader, id(3), None, State.Processing, time(3), author, None),
      Record("1", loader, id(4), id(3).some, State.Processed, time(4), author, None))

    val presentIdIsExcluded = TestManifest
      .query(Application("rdb-shredder", "0.13.0").some, loader.some)
      .compile.toList
      .run(records) must beRight(List.empty)
    val absentIdIsIncluded = TestManifest
      .query(Application("rdb-shredder", "0.13.0").some, loader.copy(instanceId = Some("id2")).some)
      .compile.toList
      .run(records) must beRight(List("1"))

    presentIdIsExcluded and absentIdIsIncluded
  }

  def e11 = {
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")

    val itemOne = List(
      Record("1", Application(agent, None), id(0), None, State.New, time(0), author, None),
      Record("1", Application(agent, None), id(1), id(0).some, State.Processing, time(1), author, None),
      Record("1", Application(agent, None), id(2), id(1).some, State.Processed, time(2), author, None))

    val itemTwo = List(
      Record("2", Application(agent, None), id(3), None, State.New, time(3), author, None),
      Record("2", Application(agent, None), id(4), id(3).some, State.Processing, time(4), author, None),
      Record("2", Application(agent, None), id(5), id(4).some, State.Processed, time(5), author, None))

    val records = itemOne ++ itemTwo

    TestManifest
      .query(Application("rdb-shredder", "0.13.0").some, None)
      .compile.toList
      .run(records) must beRight(List("1", "2"))
  }

  def e12 = {
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")
    val loader = Application(Agent("rdb-loader", "0.14.0"), Some("id1"))

    val itemOne = List(
      Record("1", Application(agent, None), id(0), None, State.New, time(0), author, None),
      Record("1", Application(agent, None), id(1), id(0).some, State.Processing, time(1), author, None),
      Record("1", Application(agent, None), id(2), id(1).some, State.Processed, time(2), author, None))

    val itemTwo = List(
      Record("2", Application(agent, None), id(3), None, State.New, time(3), author, None),
      Record("2", Application(agent, None), id(4), id(3).some, State.Processing, time(4), author, None),
      Record("2", Application(agent, None), id(5), id(4).some, State.Processed, time(5), author, None))

    val records = itemOne ++ itemTwo

    TestManifest
      .query(None, loader.some)   // without preparedBy restriction Loader can process anything that it didn't process yet
      .compile.toList
      .run(records) must beRight(List("1", "2"))
  }

  /** This property is important so that operators won't forget to recover an Item */
  def e13 = {
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")
    val shredder = Application(agent, None)

    val records = List(
      Record("1", shredder, id(0), None, State.New, time(0), author, None),
      Record("1", shredder, id(1), id(0).some, State.Processing, time(1), author, None),
      Record("1", shredder, id(2), id(1).some, State.Failed, time(2), author, None)
    )
    val lockingRecord = NonEmptyList(records.get(2).get, Nil)

    val idsForLoader = TestManifest.query(None, Application("rdb-loader", "0.13.0").some)

    val newAppExpectation = TestManifest
      .getUnprocessed(idsForLoader, _ => true)
      .run(records) must beLeft(ManifestError.Locked(lockingRecord, None))

    newAppExpectation
  }

  /**
    * Unlike getUnprocessed, processNewItem does not care about about previous failures,
    * so starting apps (transformers) can add data, which will be processed by second apps
    * (loaders) once failure is resolved
    */
  def e14 = {
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")

    def process: ProcessingManifest.ProcessNew =
      () => scala.util.Success(None)

    val records = List(
      Record("1", Application(agent, None), id(0), None, State.New, time(0), author, None),
      Record("1", Application(agent, None), id(1), id(0).some, State.Processing, time(1), author, None),
      Record("1", Application(agent, None), id(2), id(1).some, State.Failed, time(2), author, None)
    )

    TestManifest.processNewItem("2", Application("rdb-shredder", "0.14.0"), None, process).run(records) must beRight(())
  }

  /** This property does not hold because we query *already processed by*
    * but would be nice to have eager failure check for performance reasons
    */
  def e15 = {
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")
    val shredder = Application(agent, None)

    val records = List(
      Record("1", shredder, id(0), None, State.New, time(0), author, None),
      Record("1", shredder, id(1), id(0).some, State.Processing, time(1), author, None),
      Record("1", shredder, id(2), id(1).some, State.Failed, time(2), author, None)
    )
    val lockingRecord = NonEmptyList(records.get(2).get, Nil)

    val idsForLoader = TestManifest.query(shredder.some, Application("rdb-loader", "0.13.0").some)

    val newAppExpectation = TestManifest
      .getUnprocessed(idsForLoader, _ => true)
      .run(records) must beLeft(ManifestError.Locked(lockingRecord, None))

    skipped("Implement in next version")
  }
}

