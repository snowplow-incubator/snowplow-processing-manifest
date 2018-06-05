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

import scala.language.higherKinds

import java.io.{ StringWriter, PrintWriter }
import java.util.UUID

import cats.Show
import cats.implicits._
import cats.data.NonEmptyList

/** ADT representing error states */
sealed trait ManifestError

object ManifestError {
  /**
    * Inconsistent state, such as holding lock. Recoverable, considered normal flow
    * @param reason records that don't have duality (e.g. `Failed` without `Resolved`),
    *               meaning that they were not consumed and became a reason of stop
    * @param acquired `Processing` with `id` that was added before second check due race condition.
    *                This "lock" is safe to delete
    */
  final case class Locked(reason: NonEmptyList[Record], acquired: Option[UUID]) extends ManifestError

  /** Unreachable storage, timeout etc. Usually recoverable error */
  final case class IoError(message: String) extends ManifestError

  /** Corrupted state, unknown field, missing field etc. Usually critical */
  final case class Corrupted(reason: Corruption) extends ManifestError

  /** Auxiliary ADT to represent exact reason of failure; Usually impossible */
  sealed trait Corruption

  object Corruption {
    /** Item was in manifest in last request, but disappeared */
    final case class ItemLost(item: Item) extends Corruption
    /** Item was parsed incorrectly, e.g. with two `ids` or Iglu validation failed */
    final case class InvalidContent(errors: NonEmptyList[String]) extends Corruption
    /** Some field in `Record` has unexpected format */
    final case class ParseError(message: String) extends Corruption
    /** The only common `Corruption` type. User requested impossible action, e.g. process Skipped item */
    final case class InvalidRequest(message: String) extends Corruption
  }

  /** Exception happened during item processing. Not manifest error, technically */
  final case class ApplicationError(underlying: Throwable, app: Application, runId: UUID) extends ManifestError

  def parseError(message: String): ManifestError =
    Corrupted(Corruption.ParseError(message))

  def invalidContent(message: String): ManifestError =
    Corrupted(Corruption.InvalidContent(NonEmptyList.one(message)))

  import Corruption.{InvalidContent, ItemLost, ParseError, InvalidRequest}

  implicit val manifestErrorShow: Show[ManifestError] =
    new Show[ManifestError] {
      def show(t: ManifestError): String = t match {
        case Locked(reason, None) =>
          s"Manifest is locked and app cannot proceed due following unconsumed records ${reason.toList.mkString(", ")} " +
            "Add `Resolved` record to proceed"
        case Locked(reason, Some(acquired)) =>
          s"Manifest is locked and app cannot proceed due following unconsumed records ${reason.toList.mkString(", ")} " +
            s"This is probably happened due race condition for $acquired. Safe to add `Resolved` record"
        case IoError(message) =>
          s"IO Error during communication with manifest. Contact system administrator\n$message"
        case ApplicationError(throwable, app, runId) =>
          val sw = new StringWriter()
          val pw = new PrintWriter(sw)
          throwable.printStackTrace(pw)
          show"Application ${app.show} failed during $runId run. Action should be taken by operator. Exception details:\n ${sw.toString}"
        case Corrupted(InvalidRequest(error)) =>
          s"Invalid manifest request. $error"

        // Following are impossible without manual manifest intervention
        case Corrupted(ItemLost(item)) =>
          s"Invalid manifest state. Item ${item.id} disappeared between two requests. Contact developers. " +
            s"Full lost item info: $item"
        case Corrupted(InvalidContent(errors)) =>
          "Invalid manifest state. Item was constructed with unexpected content or content wasn't validated properly." +
            show"Contact developers. ${errors.toList.mkString(",")}"
        case Corrupted(ParseError(errors)) =>
          show"Invalid manifest state. Some entity has unexpected content. Contact developers. \n$errors"
      }
    }
}
