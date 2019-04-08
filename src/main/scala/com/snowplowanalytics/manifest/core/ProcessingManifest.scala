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
import cats.effect._
import cats.data.NonEmptyList
import cats.implicits._

import fs2._

import com.snowplowanalytics.iglu.client.Resolver

import ProcessingManifest._
import ManifestError._

/**
  * Base trait for processing manifest functionality
  * @tparam F effect producing by interaction with manifest
  */
abstract class ProcessingManifest[F[_]](val resolver: Resolver[F])
                                       (implicit private[manifest] val F: ManifestAction[F]) {
  /** Add an atomic record to manifest */
  def put(itemId: ItemId,
          app: Application,
          previousRecordId: Option[UUID],
          state: State,
          author: Option[Agent],
          payload: Option[Payload]): F[(UUID, Instant)]

  /** Get state of single item, with validating state of `Item` */
  def getItem(id: ItemId): F[Option[Item]]

  /**
    * Get ids of items that were processed by `processedBy` and NOT processed by `application`
    * Most common kind of query for Processing Manifest.
    */
  def query(preparedBy: Option[Application], requester: Option[Application])
           (implicit S: Sync[F]): Stream[F, ItemId] = {
    val emptySet = Stream.emit(Set.empty[ItemId]).covary[F]
    for {
      skippedItems <- fetch(requester, State.Skipped.some)
        .through(materialize)

      alreadyProcessedStream = fetch(requester, State.Processed.some)
        .filter(!skippedItems.contains(_))
        .through(materialize)
      alreadyProcessedItems <- requester.fold(emptySet)(_ => alreadyProcessedStream)

      preparedStream = fetch(preparedBy, State.Processed.some)
        .filter(!skippedItems.contains(_))
        .through(materialize)
      preparedItems <- preparedBy.fold(emptySet)(_ => preparedStream)

      allItems <- fetch(None, State.New.some)
        .filter(!skippedItems.contains(_))
        .filter(!alreadyProcessedItems.contains(_))
        .filter(id => if (preparedBy.isEmpty) true else preparedItems.contains(id))
    } yield allItems
  }

  def fetch(processedBy: Option[Application], state: Option[State])
           (implicit F: ManifestAction[F], S: Sync[F]): Stream[F, ItemId]

  /** Get full manifest */
  def stream(implicit S: Sync[F]): Stream[F, Record]

  /**
    * Get items from collection of ids that:
    * + were processed by `preprocessor` application
    * + were NOT yet processed by specified `application`
    * + match `predicate` function
    * + not in "blocked" or "failed" state
    * @param predicate filter function to get only valid `Item`s,
    *                  e.g. containing particular payload
    */
  def getUnprocessed(itemIds: Stream[F, ItemId],
                     predicate: Item => Boolean)
                    (implicit S: Sync[F]): F[List[Item]] = {


    val NotFoundError = F.raiseError[Item](ManifestError.invalidContent("Cannot find previously existing ItemId"))
    val aggregated = itemIds
      .evalMap(getItem)
      .evalMap(_.fold(NotFoundError)(F.pure))
      .map(item => item -> StateCheck.inspect(item))
      .fold(QueryResult.emptyQueryResult) {
        case (QueryResult(blocked, items), (item, StateCheck.Ok)) if predicate(item) =>
          QueryResult(blocked, item :: items)
        case (QueryResult(blocked, items), (_, StateCheck.Blocked(record))) =>
          QueryResult(record :: blocked, items)
        case (result, (_, StateCheck.Ok)) =>
          result
      }

    S.flatMap(aggregated.compile.toList) { Foldable[List].fold(_) match {
      case QueryResult(h :: t, _) =>
        val error: ManifestError = Locked(NonEmptyList(h, t), None)
        F.raiseError[List[Item]](error)
      case QueryResult(_, toProcess) =>
        F.pure(toProcess)
    } }
  }

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

    for {
      newItem <- getItemOrAdd(id, app)
      (item, _) = newItem
      _ <- processItem(app, acquirePayload, wrappedProcess)(item)
    } yield ()
  }

  /** Acquire lock, apply processing function and write its result back to manifest */
  def processItem(app: Application, acquirePayload: Option[Payload], process: Process)(item: Item): F[(UUID, Instant)] =
    processItemWithHandler[F](LockHandler.Default(this))(app, acquirePayload, process)(item)

  /**
    * Apply `Process` function to all items unprocessed by `app`
    * For each item, lock will be held. If any of items already holding a lock,
    * function breaks immediately
    */
  def processAll(app: Application,
                 predicate: Item => Boolean,
                 acquirePayload: Option[Payload],
                 process: Process)
                (implicit S: Sync[F]): F[Unit] = {
    implicit val F: Functor[F] = S
    for {
      // We don't care be what apps item was already processed
      records <- getUnprocessed(query(None, Some(app)), predicate)
      _ <- records.traverse[F, (UUID, Instant)](processItem(app, acquirePayload, process))
    } yield ()
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
          val result = ifUnprocessed[F](item, i => !Item.processedBy(application, i) && Item.canBeProcessedBy(application)(i))
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
}

object ProcessingManifest {

  /**
    * Core manifest-interaction type. It needs to have an ability to express
    * error in terms of `ManifestError` ADT without throwing exceptions
    */
  type ManifestAction[F[_]] = MonadError[F, ManifestError]

  /** Global manifests' version */
  val Version: String = generated.ProjectMetadata.version

  /** Application's main side-effecting function, processing manifest's item */
  type Process = Item => Try[Option[Payload]]

  /** Lazy function that does not expect existing `Item` (probably it'll be created due course) */
  type ProcessNew = () => Try[Option[Payload]]

  /**
    * Class holding all methods necessary to interact with manifest during processing
    * Function can be overwritten to mock IO-interactions
    * All application using `LockHandler` are supposed to be "processing applications", not "operational"
    */
  /** Workflow surrounding processing an `Item`. Lock acquisition and release */
  def processItemWithHandler[F[_]: ManifestAction](lockHandler: LockHandler[F])
                                                  (app: Application,
                                                   acquirePayload: Option[Payload],
                                                   process: Process)
                                                  (item: Item): F[(UUID, Instant)] = {
    for {
      processing        <- lockHandler.acquire(app, item, acquirePayload)
      (processingId, _)  = processing
      putPair           <- process(item) match {
        case Success(payload) =>
          lockHandler.release(app, item, processingId, payload)
        case Failure(t) =>
          for {
            putPair <- lockHandler.fail(app, item, processingId, t)
            error: ManifestError = ApplicationError(t, app, processingId)
            _ <- error.raiseError[F, Unit]
          } yield putPair
      }
    } yield putPair
  }

  /** Version of `getUnprocessed` for single retrieved Item */
  def ifUnprocessed[F[_]: ManifestAction](item: Item, predicate: Item => Boolean): F[Item] = {
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

  private case class QueryResult(blocked: List[Record], toProcess: List[Item])

  private object QueryResult {
    val emptyQueryResult = QueryResult(Nil, Nil)

    def merge(a: QueryResult, b: QueryResult): QueryResult =
      QueryResult((a.blocked ++ b.blocked).distinct, (a.toProcess ++ b.toProcess).distinct)

    implicit val monoid: Monoid[QueryResult] = new Monoid[QueryResult] {
      def empty = emptyQueryResult
      def combine(x: QueryResult, y: QueryResult) = merge(x, y)
    }
  }
}
