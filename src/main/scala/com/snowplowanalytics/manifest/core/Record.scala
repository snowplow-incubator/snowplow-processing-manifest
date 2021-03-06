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

import java.time.Instant
import java.util.UUID

import io.circe._
import io.circe.syntax._
import io.circe.java8.time._

import cats.{Order, Show, Monad}
import cats.data.EitherT
import cats.implicits._
import cats.effect.Clock

import com.snowplowanalytics.iglu.client.{ Resolver, Client, ClientError }
import com.snowplowanalytics.iglu.client.validator.CirceValidator
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._



/**
  * Atomic unit of `Item`. Usually represented as immutable row in a DB table (manifest)
  * Each record represents some state change of `Item`
  * @param itemId string, uniquely identifying set of events (`Item`) e.g. FS path
  * @param application application saved this `Record`
  * @param recordId record uniq id
  * @param previousRecordId "parent" record, being consumed but this record,
  *                         e.g. parent - `PROCESSING`, this - `FAILED`
  * @param timestamp application's **wallclock** timestamp. Note that this
  *                  can be arbitrary, nothing guarantees its correctness
  * @param author application that **actually** added the record (imposter)
  * @param payload application's payload that can be used by
  *                subsequent applications/states
  */
final case class Record(itemId: ItemId,
                        application: Application,
                        recordId: UUID,
                        previousRecordId: Option[UUID],
                        state: State,
                        timestamp: Instant,
                        author: Author,
                        payload: Option[Payload]) {
  /** Check that payload conforms its schema */
  private[core] def ensurePayload[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F]): EitherT[F, String, Unit] = payload match {
    case None => ().asRight.toEitherT[F]
    case Some(json) => Client(resolver, CirceValidator).check(json).leftMap(_.asJson.noSpaces)
  }


  /** Human-readable representation */
  def show: String =
    s"""|Item: $itemId
        |Id: $recordId
        |State: ${state.show}
        |Application: ${application.show}""" ++ payload.fold("")(p => s"\nPayload:\n${p.data.spaces2}")
}

object Record {

  private[core] val ItemIdKey = "itemId"
  private[core] val ApplicationKey = "application"
  private[core] val StateKey = "state"
  private[core] val RecordIdKey = "recordId"
  private[core] val PreviousRecordIdKey = "previousRecordId"
  private[core] val TimestampKey = "timestamp"
  private[core] val AuthorKey = "author"
  private[core] val PayloadKey = "payload"

  implicit val ordering: Order[Record] =
    implicitly[Order[Long]].contramap[Record](_.timestamp.toEpochMilli)

  implicit val jsonEncoder: Encoder[Record] = Encoder.instance[Record] {
    case Record(id, app, recordId, parent, state, timestamp, author, payload) =>
      Json.obj(
        ItemIdKey           -> Json.fromString(id),
        ApplicationKey      -> app.asJson,
        RecordIdKey         -> recordId.asJson,
        PreviousRecordIdKey -> parent.asJson,
        StateKey            -> state.asJson,
        TimestampKey        -> timestamp.asJson,
        AuthorKey           -> author.asJson,
        PayloadKey          -> payload.map(_.normalize).getOrElse(Json.Null)
      )
  }

  implicit val jsonDecoder: Decoder[Record] = Decoder.instance[Record] { cursor =>
    cursor.value.asObject match {
      case Some(jsonObject) =>
        def fold[A: Decoder](key: String) =
          Common.decodeKey[A](jsonObject.toMap, cursor)(key)

        for {
          i <- fold[String](ItemIdKey)
          a <- fold[Application](ApplicationKey)
          r <- fold[UUID](RecordIdKey)
          pr <- fold[Option[UUID]](PreviousRecordIdKey)
          s <- fold[State](StateKey)
          t <- fold[Instant](TimestampKey)
          m <- fold[Author](AuthorKey)
          p <- fold[Option[Json]](PayloadKey)
          d <- p match {
            case None => None.asRight
            case Some(json) => SelfDescribingData.parse(json) match {
              case Right(data) => data.some.asRight
              case Left(e) =>
                val message = s"Payload [$json] does not match self-describing JSON format, ${e.code}"
                DecodingFailure(message, cursor.history).asLeft
            }
          }
        } yield Record(i, a, r, pr, s, t, m, d)

      case None =>
        DecodingFailure("Processing Manifest Record is not a JSON object", cursor.history).asLeft
    }
  }

  implicit val recordShow: Show[Record] =
    Show.show(_.show)
}
