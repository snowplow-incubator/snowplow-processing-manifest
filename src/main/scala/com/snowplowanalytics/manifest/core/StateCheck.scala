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

import cats.implicits._

/** Possible state of Item: either Ok to process or blocked by some `Record` */
sealed trait StateCheck {
  def merge(other: StateCheck): StateCheck =
    (this, other) match {
      case (StateCheck.Ok, StateCheck.Ok) => StateCheck.Ok
      case (b: StateCheck.Blocked, _) => b
      case (_, b: StateCheck.Blocked) => b
    }
}

object StateCheck {
  /** Good to process */
  case object Ok extends StateCheck

  /**
    * Notify operator: either locked or failed
    * @param record "unconsumed" record. `Processing` or `Failed`
    */
  case class Blocked(record: Record) extends StateCheck

  // Current behavior assumes that "locked" and "failed" are identical
  // This shouldn't be the case in many pipelines,
  // E.g. transformer should proceed to new folders when others are in processing

  /**
    * Check that Processing/Failed record is "consumed"
    * e.g. item contains its duality (Processed/Resolved) with same id
    * Processing can also be "consumed" by Failed
    */
  def consumed(item: Item)(record: Record): Option[Boolean] = {
    val states = item.records.toList.map(r => (r.previousRecordId, r.state))
    record.state match {
      case State.Processing =>
        val processed = states.contains((record.recordId.some, State.Processed))
        val failed = states.contains((record.recordId.some, State.Failed))
        Some(processed || failed)
      case State.Failed =>
        Some(states.contains((record.recordId.some, State.Resolved)))
      case _ => None
    }
  }

  /**
    * Check state of `Item`, e.g. it is locked or blocked by failure
    * It cannot decide whether item skipped or not:w
    */
  def inspect(item: Item): StateCheck = {
    val closed = consumed(item)(_: Record).getOrElse(false)

    item.records.foldLeft(Ok: StateCheck) { (action, record) =>
      record.state match {
        case State.New                          => action.merge(Ok)
        case State.Processing if closed(record) => action.merge(Ok)
        case State.Processing                   => action.merge(StateCheck.Blocked(record))
        case State.Processed                    => action.merge(Ok)
        case State.Failed     if closed(record) => action.merge(Ok)
        case State.Failed                       => action.merge(StateCheck.Blocked(record))
        case State.Skipped                      => action.merge(StateCheck.Ok)
        case State.Resolved                     => action.merge(Ok)
      }
    }
  }
}

