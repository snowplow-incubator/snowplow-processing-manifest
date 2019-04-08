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
package com.snowplowanalytics

import cats.MonadError
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.Sync

import io.circe.Json

import fs2._

import com.snowplowanalytics.iglu.core.SelfDescribingData

import manifest.core._
import manifest.core.ProcessingManifest.ManifestAction

package object manifest {
  type Payload = SelfDescribingData[Json]

  def materialize[F[_], A](implicit F: Sync[F]): Pipe[F, A, Set[A]] =
    (s: Stream[F, A]) => Stream.eval(s.compile.to[Set])

  /** Catch all unexpected exceptions into Manifest-specific IO error */
  def convert[F[_], A, B](stream: Stream[F, A], compile: Stream[F, A] => F[B])
                         (implicit F: Sync[F], E: ManifestAction[F]): F[B] = {
    val result = F.attempt(compile(stream))
    E.flatMap(result) {
      case Right(x) => E.pure(x)
      case Left(e) => E.raiseError(ManifestError.IoError(e.getMessage))
    }
  }

  /** Helper method to flatten `Either` (e.g. in case of parse-error) */
  implicit class FoldFOp[A](either: Either[ManifestError, A]) {
    def foldF[F[_]](implicit F: MonadError[F, ManifestError]): F[A] =
      either.fold(_.raiseError[F, A], _.pure[F])
  }

  /** Id representation, such as S3 URI */
  type ItemId = String

  /** All items, grouped by their id */
  type ManifestMap = Map[ItemId, Item]

  /** Get full manifest with items grouped by their id, without validating state of `Item` */
  implicit class UnsafeManifestWrapper[F[_]](manifest: ProcessingManifest[F]) {
    def items(implicit S: Sync[F], F: ManifestAction[F]): F[ManifestMap] = {
      val fetched = convert(manifest.stream, (s: Stream[F, Record]) => s.compile.toList)
      S.map(fetched) { records =>
        // Replace with .groupByNel in 1.0.1
        records.groupBy(_.itemId).map { case (k, list) => list match {
          case h :: t => (k, Item(NonEmptyList(h, t))).some
          case Nil => none[(String, Item)]
        }}.toList.sequence.map(_.toMap).getOrElse(Map.empty)
      }
    }
  }

}
