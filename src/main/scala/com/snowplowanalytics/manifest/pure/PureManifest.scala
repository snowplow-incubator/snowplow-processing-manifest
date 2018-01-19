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
package pure

import java.util.UUID
import java.time.Instant

import cats.data.{EitherT, NonEmptyList, StateT}
import cats.effect.{IO, Sync}
import cats.implicits._

import com.snowplowanalytics.iglu.client.Resolver

import core._
import core.ProcessingManifest._
import PureManifest._

/**
  * Manifest implementation keeping all records in `State` monad.
  * IO in type-signature should be ignored as required only by `fs2.Stream#compile`
  * Use for tests only!
  */
case class PureManifest(override val resolver: Resolver) extends ProcessingManifest[PureManifestEffect](resolver) with PureManifest.Impl {

  def apply(records: List[Record]): PureManifestEffect[List[Record]] =
    EitherT.right(StateT.set[IO, List[Record]](records)).map(_ => records)

  def fetch(processedBy: Option[Application], state: Option[State])
           (implicit F: ManifestAction[PureManifestEffect], S: Sync[PureManifestEffect]): fs2.Stream[PureManifestEffect, ItemId] = {
    stream
      .filter(r => state.forall(_ == r.state))
      .filter(record => processedBy match {
        case Some(application) => Item.inState(application, record, state)
        case None => true
      })
      .map(_.itemId)
  }

  def put(itemId: ItemId,
          app: Application,
          parentRecordId: Option[UUID],
          step: core.State,
          author: Option[Agent],
          payload: Option[Payload]): PureManifestEffect[(UUID, Instant)] =
    EitherT.right(putS(itemId, app, parentRecordId, step, author, payload))

  def stream(implicit S: Sync[PureManifestEffect]): fs2.Stream[PureManifestEffect, Record] = {
    val recordsState: PureManifestEffect[List[Record]] = EitherT.right[ManifestError](listS)
    for {
      list <- fs2.Stream.eval[PureManifestEffect, List[Record]](recordsState)
      record <- fs2.Stream.emits(list).covary[PureManifestEffect]
    } yield record
  }

  def getItem(id: ItemId): PureManifestEffect[Option[Item]] = {
    val itemS = for {
      i <- getItemS(id)
    } yield i match {
      case Some(ii) => ii.ensure[Either[ManifestError, ?]](resolver).map(_.some)
      case None => none[Item].asRight
    }

    EitherT[ManifestState[List[Record], ?], ManifestError, Option[Item]](itemS)
  }
}

object PureManifest {

  // IO is temporary here, due unnecessary Sync restriction from ProcessingManifest
  type ManifestState[S, A] = StateT[IO, S, A]

  object ManifestState {
    def apply[S, A](f: S => (S, A)): ManifestState[S, A] =
      StateT[IO, S, A](s => IO(f(s)))
  }

  type PureManifestEffect[A] = EitherT[ManifestState[List[Record], ?], ManifestError, A]

  trait Impl {

    val StartTime = Instant.ofEpochMilli(1524870034204L)

    def getItemS(id: ItemId): ManifestState[List[Record], Option[Item]] = {
      ManifestState((records: List[Record]) => {
        val item = records.filter(_.itemId == id) match {
          case Nil => None
          case h :: t => Some(Item(NonEmptyList(h, t)))
        }
        (records, item)
      })
    }

    def putS(itemId: ItemId,
             app: Application,
             parentRecordId: Option[UUID],
             step: core.State,
             author: Option[Agent],
             payload: Option[Payload]): ManifestState[List[Record], (UUID, Instant)] = {

      val a = Author(author.getOrElse(app.agent), ProcessingManifest.Version)
      def f(records: List[Record]) = {
        val id = seed(records.toSet)
        val timestamp = time(records.toSet)
        val newRecord = Record(itemId, app, id, parentRecordId, step, timestamp, a, payload)
        (newRecord :: records, (id, timestamp))
      }
      ManifestState(f)
    }

    def listS: ManifestState[List[Record], List[Record]] = {
      def f(records: List[Record]) = (records, records)
      ManifestState(f)
    }

    def id(num: Int): UUID = UUID.nameUUIDFromBytes(Array(java.lang.Byte.valueOf(num.toString)))

    def seed(set: Set[_]): UUID = id(set.size)
    def time(set: Set[_]): Instant = time(set.size)
    def time(int: Int): Instant =  StartTime.plusSeconds(int)

    implicit class PureManifestRunner[A](action: PureManifestEffect[A]) {
      def run(records: List[Record]): Either[ManifestError, A] =
        runWithState(records)._2

      def runWithState(records: List[Record]): (List[Record], Either[ManifestError, A]) =
        action.value.run(records).unsafeRunSync()
    }
  }
}
