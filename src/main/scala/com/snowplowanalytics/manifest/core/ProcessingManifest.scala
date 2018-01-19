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

import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

import java.time.Instant
import java.util.UUID

import cats._
import cats.data.NonEmptyList
import cats.implicits._

import com.snowplowanalytics.iglu.client.Resolver

import ProcessingManifest._
import ManifestError._

/**
  * Base trait for processing manifest functionality
  * @tparam F effect producing by interaction with manifest
  */
abstract class ProcessingManifest[F[_]](val resolver: Resolver)(implicit private[manifest] val F: ManifestAction[F]) {
  /** Add an atomic record to manifest */
  def put(itemId: ItemId,
          app: Application,
          previousRecordId: Option[UUID],
          state: State,
          author: Option[Agent],
          payload: Option[Payload]): F[(UUID, Instant)]

  /** Get state of single item, with validating state of `Item` */
  def getItem(id: ItemId): F[Option[Item]]

  /** Get full manifest */
  def list: F[List[Record]]

  /** Get full manifest with items grouped by their id, without validating state of `Item` */
  def items: F[ManifestMap] = list.map { records =>
    // Replace with .groupByNel in 1.0.1
    records.groupBy(_.itemId).map { case (k, list) => list match {
      case h :: t => (k, Item(NonEmptyList(h, t))).some
      case Nil => none[(String, Item)]
    }}.toList.sequence.map(_.toMap).getOrElse(Map.empty)
  }

  /** Short-hand to get all unprocessed items */
  def unprocessed(application: Application, predicate: Item => Boolean): F[List[Item]] =
    ProcessingManifest.unprocessed(items, application, predicate)

  /**
    * Similar to `processItem`, but works without existing `Item`,
    * instead it creates new one itself or queries it by known `id`
    * Should be used when `app` is both adding item to manifest
    * and processes it straight afterwards
    * @param id new item id, which should be either fetched or created
    * @param app application doing discovering and processing
    * @param acquirePayload optional payload that will be added as part of `Processing`,
    *                       can be e.g. notification that `app` started to process it
    *                       with some special configuration (cross-batch dedupe enabled)
    * @param process Process that does not expect existing item
    */
  def processNewItem(id: ItemId, app: Application, acquirePayload: Option[Payload], process: ProcessNew): F[Unit] = {
    val wrappedProcess: Process = (_: Item) => process()

    // Created handler will either know existing id or will add new one
    def getHandler(item: Item, created: Boolean): F[LockHandler[F]] =
      item.records.find(_.state == State.New).map(_.recordId) match {
        case Some(i) if created =>
          val getId = () => i.pure[F]
          LockHandler.copy(getId = getId).pure[F]
        case Some(_) =>
          LockHandler.pure[F]
        case None =>
          val error = "'processNewItem' was called on item without NEW record"
          ManifestError.invalidContent(error).raiseError[F, LockHandler[F]]
      }

    for {
      newItem <- getItemOrAdd(id, app)
      (item, created) = newItem
      handler <- getHandler(item, created)
      _ <- processItemWithHandler[F](handler)(app, acquirePayload, wrappedProcess)(item)
    } yield ()
  }

  /** Acquire lock, apply processing function and write its result back to manifest */
  def processItem(app: Application, acquirePayload: Option[Payload], process: Process)(item: Item): F[(UUID, Instant)] =
    processItemWithHandler[F](LockHandler)(app, acquirePayload, process)(item)

  /**
    * Apply `Process` function to all unprocessed events
    * For each item, lock will be held. If any of items already holding a lock,
    * function breaks immediately
    */
  def processAll(app: Application,
                 predicate: Item => Boolean,
                 acquirePayload: Option[Payload],
                 process: Process): F[Unit] = {

    for {
      items <- unprocessed(app, predicate)
      _ <- items.traverse[F, (UUID, Instant)](processItem(app, acquirePayload, process))
    } yield ()
  }

  /** Helper to release lock (put `Processed`) */
  final def release(app: Application, item: Item, previous: UUID, payload: Option[Payload]): F[(UUID, Instant)] =
    put(item.id, app, previous.some, State.Processed, None, payload)

  /** Helper to mark `Item` as `Failed` (put `Failed`) */
  final def fail(app: Application, item: Item, previous: UUID, throwable: Throwable): F[(UUID, Instant)] = {
    val payload = Payload.exception(throwable)
    put(item.id, app, previous.some, State.Failed, None, Some(payload))
  }

  /** Default handler, adding `Processing` for acquire and `Processed` for release */
  val LockHandler: LockHandler[F] =
    ProcessingManifest.LockHandler[F](() => UUID.randomUUID().pure[F], acquire, release, fail)

  /**
    * Acquire lock by adding `Processing` state to the `Item`
    * Only processing-apps should acquire lock (no snowplowctl)
    *
    * It performs two Item-checks: one *before* acquiring lock, to check that
    * application is still working with unchanged item (some time might pass
    * since we received `Item` - it even could happen in batch);
    * one *after* acquiring lock, to check that meanwhile `put`-request was sent
    * no other application attempted to write to this item
    * It acquires lock for particular application, but will also fail because of
    * "race condition" if any other application attempted to write
    * @param app application that tries to acquire lock
    * @param item original unprocessed item, from batch of `items`
    * @param payload payload that will be add to `Processing` record
    * @return time of adding `Processing` within `MonadError` effect, telling if acquisition was successful
    */
  private[manifest] def acquire(app: Application, item: Item, payload: Option[Payload]): F[(UUID, Instant)] = {
    val result = for {
      _      <- checkConsistency(item, None)
      result <- put(item.id, app, none, State.Processing, None, payload)
      (id, _) = result
      _      <- checkConsistency(item, Some(id))
    } yield result

    result.onError {
      // If another app managed to add `Processing` - mark it as `Failed`
      case Locked(_, Some(blockingRecord)) =>
        put(item.id, app, none, State.Failed, None, Some(Payload.locked(blockingRecord))).void
    }
  }

  /**
    * Check if `Item` exists already, if yes - return existing; if no - create new one
    * @param id item id to use in query
    * @param application app that should be claimed as a record-owner
    * @return either existing or newly created (with only `New` record) item
    *         and flag if item was actually created
    */
  private[manifest] def getItemOrAdd(id: ItemId, application: Application): F[(Item, Boolean)] = {
    for {
      existing <- getItem(id)
      result   <- existing match {
        case Some(item) =>
          val result = checkState[F](item, i => !Item.processedBy(application, i) && Item.canBeProcessedBy(application)(i))
          result.map(i => (i, false))
        case None =>
          val authorApp = Author(application.agent, Version)
          for {
            putResult <- put(id, application, none, State.New, authorApp.agent.some, None)
            (recordId, timestamp) = putResult
            record     = Record(id, application, recordId, none, State.New, timestamp, authorApp, None)
            fetched   <- getItem(id)
            expected   = Item(NonEmptyList.one(record))
            result    <- if (fetched.contains(expected)) {
              // Without acquiring lock this check is fruitless
              expected.pure[F]
            } else {
              val message = s"New Item $existing is not equal to expected $expected"
              ManifestError.invalidContent(message).raiseError[F, Item]
            }
          } yield (result, true)
      }
    } yield result
  }


  /**
    * Check that storage still holds the same `Item`,
    * nothing changed it after original `Item` was fetched
    * @param item original list of records
    * @param added one record that has been added since last check
    */
  private[manifest] def checkConsistency(item: Item, added: Option[UUID]): F[Item] = {

    def isConsistent(freshItem: Item): F[Unit] = added match {
      case None =>       // First check
        checkEmptiness(freshItem.records.toList.diff(item.records.toList))
      case Some(id) =>   // Second check
        checkEmptiness(freshItem.records.filterNot(_.recordId == id).diff(item.records.toList))
    }

    def checkEmptiness(records: List[Record]): F[Unit] = records match {
      case Nil => ().pure[F]
      case h :: t =>
        val error: ManifestError = Locked(NonEmptyList(h, t), added)
        error.raiseError[F, Unit]
    }

    def isExisting(freshItem: Option[Item]): F[Item] = freshItem match {
      case Some(existing) => existing.pure[F]
      case None =>
        val error: ManifestError = Corrupted(Corruption.ItemLost(item))
        error.raiseError[F, Item]
    }

    for {
      freshItem <- getItem(item.id)
      current   <- isExisting(freshItem)
      _         <- isConsistent(current)
    } yield current
  }
}

object ProcessingManifest {

  /**
    * Core manifest-interaction type. It needs to have an ability to express
    * error in terms of `ManifestError` ADT without throwing exceptions
    */
  type ManifestAction[F[_]] = MonadError[F, ManifestError]

  /** Id representation, such as S3 URI */
  type ItemId = String

  /** Global manifests' version */
  val Version: String = generated.ProjectMetadata.version

  /** All items, grouped by their id */
  type ManifestMap = Map[ItemId, Item]

  /** Application's main side-effecting function, processing manifest's item */
  type Process = Item => Try[Option[Payload]]

  /** Lazy function that does not expect existing `Item` (probably it'll be created due course) */
  type ProcessNew = () => Try[Option[Payload]]

  /** Wrapper for function that performs `put` and can add some payload of `A` */
  type PutFunction[F[_], A] = (Application, Item, UUID, A) => F[(UUID, Instant)]

  /**
    * Class holding all methods necessary to interact with manifest during processing
    * Function can be overwritten to mock IO-interactions
    * All application using `LockHandler` are supposed to be "processing applications", not "operational"
    */
  case class LockHandler[F[_]](getId: () => F[UUID],
                               acquire: (Application, Item, Option[Payload]) => F[(UUID, Instant)],
                               release: PutFunction[F, Option[Payload]],
                               fail: PutFunction[F, Throwable])

  /** Workflow surrounding processing an `Item`. Lock acquisition and release */
  def processItemWithHandler[F[_]: ManifestAction](lockHandler: LockHandler[F])
                                                  (app: Application,
                                                   acquirePayload: Option[Payload],
                                                   process: Process)
                                                  (item: Item): F[(UUID, Instant)] = {
    for {
      id      <- lockHandler.getId()
      _       <- lockHandler.acquire(app, item, acquirePayload)
      putPair <- process(item) match {
        case Success(payload) =>
          lockHandler.release(app, item, id, payload)
        case Failure(t) =>
          for {
            putPair <- lockHandler.fail(app, item, id, t)
            error: ManifestError = ApplicationError(t, app, id)
            _ <- error.raiseError[F, Unit]
          } yield putPair
      }
    } yield putPair
  }

  /**
    * Get items that were NOT yet processed by specified application
    * and not in "blocked" or "failed" state
    * @param itemsAction all items to check
    * @param predicate filter function to get only valid `Item`s,
    *                  e.g. processed by transformer, but not processed by loader
    * @return list of `Item` that were not yet processed by `application`
    */
  def unprocessed[F[_]: ManifestAction](itemsAction: F[ManifestMap],
                                        application: Application,
                                        predicate: Item => Boolean): F[List[Item]] = {
    // Defensive predicate, ensuring that current application has not been applied yet
    def unprocessedPredicate(i: Item) =
      !Item.processedBy(application, i) && predicate(i) && Item.canBeProcessedBy(application)(i)

    itemsAction.flatMap { stateMap =>
      val itemMap = stateMap.map { case (_, i) => (i, StateCheck.inspect(i)) }

      val lockingRecords = itemMap.collect {
        case (_, StateCheck.Blocked(record)) => record
      }.toList

      lockingRecords match {
        case Nil =>
          val toProcess = itemMap.collect {
            case (i, StateCheck.Ok) if unprocessedPredicate(i) => i
          }
          toProcess.toList.pure[F]
        case h :: t =>
          val error: ManifestError = Locked(NonEmptyList(h, t), None)
          error.raiseError[F, List[Item]]
      }
    }
  }

  /** Version of `unprocessed` for single retrieved Item */
  def checkState[F[_]: ManifestAction](item: Item, predicate: Item => Boolean): F[Item] = {
    StateCheck.inspect(item) match {
      case StateCheck.Ok if predicate(item) => item.pure[F]
      case StateCheck.Ok =>
        val message = "Requested item already exists and cannot be processed by this application"
        val error: ManifestError = ManifestError.Corrupted(Corruption.InvalidRequest(message))
        error.raiseError[F, Item]
      case StateCheck.Blocked(record) =>
        val error: ManifestError = Locked(NonEmptyList(record, Nil), None)
        error.raiseError[F, Item]
    }
  }
}
