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

import java.util.UUID
import java.time.Instant

import cats.data._
import cats.implicits._

import ManifestError._
import LockHandler._

case class LockHandler[F[_]](acquire: (Application, Item, Option[Payload]) => F[(UUID, Instant)],
                             release: PutFunction[F, Option[Payload]],
                             fail: PutFunction[F, Throwable])

object LockHandler {
  /** Wrapper for function that performs `put` and can add some payload of `A` */
  type PutFunction[F[_], A] = (Application, Item, UUID, A) => F[(UUID, Instant)]

  /** Default handler, adding `Processing` for acquire and `Processed` for release */
  def Default[F[_]](manifest: ProcessingManifest[F]): LockHandler[F] =
    LockHandler[F](acquire[F](manifest), release[F](manifest), fail[F](manifest))

  /** Helper to release lock (put `Processed`) */
  final def release[F[_]](manifest: ProcessingManifest[F])
                         (app: Application, item: Item, previous: UUID, payload: Option[Payload]): F[(UUID, Instant)] =
    manifest.put(item.id, app, previous.some, State.Processed, None, payload)

  /** Helper to mark `Item` as `Failed` (put `Failed`) */
  final def fail[F[_]](manifest: ProcessingManifest[F])
                      (app: Application, item: Item, previous: UUID, throwable: Throwable): F[(UUID, Instant)] = {
    val payload = Payload.exception(throwable)
    manifest.put(item.id, app, previous.some, State.Failed, None, Some(payload))
  }

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
  def acquire[F[_]](manifest: ProcessingManifest[F])
                                     (app: Application, item: Item, payload: Option[Payload]) : F[(UUID, Instant)] = {
    import manifest._
    val result = for {
      _      <- checkConsistency(manifest)(item, None)
      result <- manifest.put(item.id, app, none, State.Processing, None, payload)
      (id, _) = result
      _      <- checkConsistency(manifest)(item, Some(id))
    } yield result

    result.onError {
      // If another app managed to add `Processing` - mark it as `Failed`
      case Locked(_, Some(blockingRecord)) =>
        manifest.put(item.id, app, none, State.Failed, None, Some(Payload.locked(blockingRecord))).void
    }
  }

  /**
    * Check that storage still holds the same `Item`,
    * nothing changed it after original `Item` was fetched
    * @param item original list of records
    * @param added one record that has been added since last check
    */
  private[manifest] def checkConsistency[F[_]](manifest: ProcessingManifest[F])
                                              (item: Item, added: Option[UUID]): F[Item] = {
    import manifest._

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
      freshItem <- manifest.getItem(item.id)
      current   <- isExisting(freshItem)
      _         <- isConsistent(current)
    } yield current
  }
}
