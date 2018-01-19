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

import cats.data.NonEmptyList
import cats.implicits._

import org.specs2.Specification

class StateCheckSpec extends Specification { def is = s2"""
  Item with single New record is Ok to process $e1
  Item with single Processing record is Blocking process $e2
  Item with Processing and unrelated Processed is still Blocking $e3
  Item with Processing and related Processed is Ok $e4
  Item with Processing and Failed is blocked by Failed $e5
  Item with Processing, Failed and Resolved is Ok $e6
  Item with Resolved, but failed with other app is Blocking $e7
  Item with Failed and Skipped for all apps is blocked $e8
  """

  def e1 = {
    val recordId = UUID.randomUUID()
    val time = Instant.now()
    val author = Author(Agent("app", "0.1.0"), "0.1.0")

    val item = Item(NonEmptyList(Record("item", Application("app", "0.1.0"), recordId, None, State.New, time, author, None), Nil))
    StateCheck.inspect(item) must beEqualTo(StateCheck.Ok)
  }

  def e2 = {
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val time = Instant.now()
    val author = Author(Agent("app", "0.1.0"), "0.1.0")

    val blockingRecord = Record("item", Application("app", "0.1.0"), id2, Some(id1), State.Processing, time, author, None)

    val item = Item(NonEmptyList(
      Record("item", Application("app", "0.1.0"), id1, None, State.New, time, author, None),
      List(blockingRecord)))

    StateCheck.inspect(item) must beEqualTo(StateCheck.Blocked(blockingRecord))
  }

  def e3 = {
    val time = Instant.now()
    val id1 = UUID.randomUUID()
    def newId = UUID.randomUUID()
    val author = Author(Agent("app", "0.1.0"), "0.1.0")

    val blockingRecord = Record("item", Application("app", "0.1.0"), newId, Some(id1), State.Processing, time, author, None)

    // Item is invalid in first place - we cannot have Processed without Processing
    val item = Item(NonEmptyList(
      Record("item", Application("app", "0.1.0"), id1, None, State.New, time, author, None),
      List(
        blockingRecord,
        Record("item", Application("app", "0.1.0"), newId, Some(newId), State.Processed, time, author, None)
      )))

    StateCheck.inspect(item) must beEqualTo(StateCheck.Blocked(blockingRecord))
  }

  def e4 = {
    val time = Instant.now()
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val id3 = UUID.randomUUID()
    val author = Author(Agent("app", "0.1.0"), "0.1.0")

    val item = Item(NonEmptyList(
      Record("item", Application("app", "0.1.0"), id1, None, State.New, time, author, None),
      List(
        Record("item", Application("app", "0.1.0"), id2, Some(id1), State.Processing, time, author, None),
        Record("item", Application("app", "0.1.0"), id3, Some(id2), State.Processed, time, author, None)
      )))

    StateCheck.inspect(item) must beEqualTo(StateCheck.Ok)
  }

  def e5 = {
    val time = Instant.now()
    val id = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val id3 = UUID.randomUUID()
    val author = Author(Agent("app", "0.1.0"), "0.1.0")

    val blockingRecord = Record("item", Application("app", "0.1.0"), id3, Some(id2), State.Failed, time, author, None)

    val item = Item(NonEmptyList(
      Record("item", Application("app", "0.1.0"), id, None, State.New, time, author, None),
      List(
        Record("item", Application("app", "0.1.0"), id2, Some(id), State.Processing, time, author, None),
        blockingRecord
      )))

    StateCheck.inspect(item) must beEqualTo(StateCheck.Blocked(blockingRecord))
  }

  def e6 = {
    val time = Instant.now()
    val id0 = UUID.randomUUID()
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val id3 = UUID.randomUUID()
    val author = Author(Agent("app", "0.1.0"), "0.1.0")

    val item = Item(NonEmptyList(
      Record("item", Application("app", "0.1.0"), id0, None, State.New, time, author, None),
      List(
        Record("item", Application("app", "0.1.0"), id1, Some(id0), State.Processing, time, author, None),
        Record("item", Application("app", "0.1.0"), id2, Some(id1), State.Failed, time, author, None),
        Record("item", Application("app", "0.1.0"), id3, Some(id2), State.Resolved, time, author, None)
      )))

    StateCheck.inspect(item) must beEqualTo(StateCheck.Ok)
  }

  def e7 = {
    val time = Instant.now()
    val state = UUID.randomUUID()
    val id = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val id3 = UUID.randomUUID()
    val id5 = UUID.randomUUID()

    val author = Author(Agent("app", "0.1.0"), "0.1.0")

    val blockingRecord = Record("item", Application("app2", "0.1.0"), id5, None, State.Failed, time, author, None)

    val item = Item(NonEmptyList(
      Record("item", Application("app", "0.1.0"), state, None, State.New, time, author, None),
      List(
        Record("item", Application("app", "0.1.0"), id, Some(state), State.Processing, time, author, None),
        Record("item", Application("app", "0.1.0"), id2, Some(id), State.Failed, time, author, None),
        Record("item", Application("app", "0.1.0"), id3, Some(id2), State.Resolved, time, author, None),

        Record("item", Application("app2", "0.1.0"), id5, None, State.Failed, time, author, None),
        blockingRecord
      )))

    StateCheck.inspect(item) must beEqualTo(StateCheck.Blocked(blockingRecord))
  }

  def e8 = {

    val time = Instant.now()
    val agent = Agent("rdb-shredder", "0.13.0")
    val author = Author(agent, "0.1.0")

    val id3 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00003")

    // Skipped items supposed to be filtered out in `unprocessed` function
    val records = List(
      Record("2", Application("rdb-shredder", "0.13.0"), UUID.randomUUID(), id3.some, State.Failed, time.plusSeconds(40), author, None),
      Record("2", Application("*", "0.13.0"), UUID.randomUUID(), None, State.Skipped, time.plusSeconds(50), author, None)
    )

    val expected = StateCheck.Blocked(records.head)
    StateCheck.inspect(Item(NonEmptyList.fromListUnsafe(records))) must beEqualTo(expected)
  }
}
