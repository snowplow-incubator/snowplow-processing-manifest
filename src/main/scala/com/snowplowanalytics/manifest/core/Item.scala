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

import com.snowplowanalytics.iglu.client.Resolver

import cats.Show
import cats.data._
import cats.implicits._

import ProcessingManifest._
import ManifestError._

/**
  * Unit of manifest consisting of multiple records related to single run id
  * Used to fold individual records and extract final app-specific item state
  */
final case class Item private[core](orderedRecords: Item.OrderedRecords) {
  /** Unboxed ordered list of Records */
  val records: NonEmptyList[Record] = orderedRecords.nel

  /** Group records by runs */
  lazy val runs: Map[UUID, List[Record]] =
    records.toList.groupBy(_.recordId)

  /** Get `id` common for all records */
  def id: ItemId = records.head.itemId

  /** Perform additional check to ensure `Item` is not corrupted */
  def ensure[F[_]: ManifestAction](resolver: Resolver): F[Item] = {
    val sorted = records.toList

    val itemIdValidation: ValidatedNel[String, Unit] = Item.checkItemId(sorted).toValidatedNel
    val payloads: ValidatedNel[String, Unit] = sorted.map(_.ensurePayload(resolver).toValidatedNel).sequence.void
    val parentIdsValidation: ValidatedNel[String, Unit] = Item.checkStateConsistency(sorted).toValidated

    (itemIdValidation, payloads, parentIdsValidation).mapN { (_, _, _) =>
      Item(NonEmptyList.fromListUnsafe(sorted))
    } match {
      case Validated.Valid(item) => item.pure[F]
      case Validated.Invalid(errors) =>
        val error: ManifestError = ManifestError.Corrupted(Corruption.InvalidContent(errors))
        error.raiseError[F, Item]
    }
  }

  override def toString: String = records.toList.toString
}

object Item {
  // Cannot leave as AnyVal because apply will have identical type after erasure
  final case class OrderedRecords(nel: NonEmptyList[Record])

  /** Common violations of FSM-nature of Item */
  sealed trait ItemFsmError

  object ItemFsmError {
    /** Referring to wrong parent state, e.g. RESOLVED cannot refer to NEW, NEW cannot refer to anything */
    case class InvalidPreviousState(record: Record, previousRecord: Option[Record]) extends ItemFsmError
    /** Has no previous record, while must have */
    case class OrphanRecord(record: Record) extends ItemFsmError

    implicit final val itemConsistencyErrorShow: Show[ItemFsmError] = new Show[ItemFsmError] {
      override def show(t: ItemFsmError): String = t match {
        case InvalidPreviousState(record, Some(previous)) =>
          s"Record ${record.recordId} with state ${record.state} refers to invalid ${previous.recordId} with state ${previous.state}"
        case InvalidPreviousState(record, None) =>
          s"Record ${record.recordId} with state ${record.state} refers to non-existent record ${record.previousRecordId.getOrElse("")}"
        case OrphanRecord(record) =>
          s"Record ${record.recordId} with state ${record.state} has no previous record"
      }
    }
  }

  /**
    * Constructor, ensuring that final `Item` always contains sorted `Record`s
    * @param unordered non-empty list (probably) received without particular order
    * @return `Item` with same `Record`s, but ordered by timestamp
    */
  def apply(unordered: NonEmptyList[Record]): Item =
    Item(OrderedRecords(NonEmptyList.fromListUnsafe(unordered.toList.sortBy(_.timestamp))))

  implicit final val itemShow: Show[Item] = new Show[Item] {
    def show(t: Item): String =
      s"Manifest Item [${t.id}] with state [${StateCheck.inspect(t)}] and records:\n${t.records.toList.mkString("\n")}"
  }

  /**
    * Primary FSM-checking state, validating that no records refer to unexpected previous records,
    * e.g. NEW is always first and FAILED always refers to some PROCESSING
    */
  final def checkStateConsistency(item: List[Record]): Either[NonEmptyList[String], Unit] = {
    import ItemFsmError._

    val remaining = item.reverse.foldLeft(List.empty[ItemFsmError]) {
      case (acc, cur) => (cur.state, cur.previousRecordId) match {
        case (State.Skipped, _)           => acc
        case (State.New, None)            => acc
        case (State.New, Some(id))        => InvalidPreviousState(cur, item.find(_.recordId == id)) :: acc
        case (State.Processing, None)     => acc
        case (State.Processing, Some(id)) if item.exists(r => r.state == State.New && r.recordId == id) => acc
        case (State.Processing, Some(id)) => InvalidPreviousState(cur, item.find(_.recordId == id)) :: acc
        case (State.Processed, None)      => OrphanRecord(cur) :: acc
        case (State.Processed, Some(id))  if item.exists(r => r.state == State.Processing && r.recordId == id) => acc
        case (State.Processed, Some(id))  => InvalidPreviousState(cur, item.find(_.recordId == id)) :: acc
        case (State.Failed, None)         => OrphanRecord(cur) :: acc
        case (State.Failed, Some(id))     if item.exists(r => r.state == State.Processing && r.recordId == id) => acc
        case (State.Failed, Some(id))     => InvalidPreviousState(cur, item.find(_.recordId == id)) :: acc
        case (State.Resolved, None)       => OrphanRecord(cur) :: acc
        case (State.Resolved, Some(id))   if item.exists(r => r.state == State.Failed && r.recordId == id) => acc
        case (State.Resolved, Some(id))   if item.exists(r => r.state == State.Processing && r.recordId == id) => acc
        case (State.Resolved, Some(id))   => InvalidPreviousState(cur, item.find(_.recordId == id)) :: acc
      } }

    remaining match {
      case Nil => ().asRight
      case orphans => NonEmptyList.fromListUnsafe(orphans.map(_.show)).asLeft
    }
  }

  /**
    * Check that `Item` was already processed by `Application`
    * Does not do any validations on `Item`, e.g. inconsistent step ids
    * won't make result different.
    * Assumes that item with `Application(_, _, None, _)` covers
    * `Application(_, _, Some(a), _)` and `Application(_, _, Some(b), _)`,
    * making safe double-processing (e.g. loading to two storage targets) by same rdb-loader
    */
  final def processedBy(application: Application, item: Item): Boolean = {
    // Special check, allowing to mark existing Items *without* argument as processed
    // against application *with* arguments.
    val argsChecker: (Boolean, (String, Option[String])) => Boolean = {
      case (acc, (currentApp, instanceId)) => acc || (currentApp == application.name && (application.instanceId match {
        case Some(_) if instanceId.isEmpty => true
        case Some(x) => instanceId.contains(x)
        case None => application.instanceId.isEmpty
      }))
    }

    item.records
      .filter(_.state == State.Processed)
      .map(s => (s.application.name, s.application.instanceId))
      .foldLeft(false)(argsChecker)
  }

  /** Check if list of applications extracted from SKIPPED record are blocking current app */
  final def canBeProcessedBy(requester: Application)(item: Item): Boolean =
    item.records.filter(_.state == State.Skipped).map(_.application).foldLeft(true) {
      (acc, cur) => cur.canBeProcessedBy(requester) && acc
    }

  /** Check that `Item` has a consistent state, e.g. it has a single id */
  final def checkItemId(item: List[Record]): Either[String, Unit] = {
    val ids = item.map(_.itemId).toSet
    if (ids.size == 1) { ().asRight }
    else { show"Single Item contains multiple ids: ${ids.mkString(",")}".asLeft }
  }
}

