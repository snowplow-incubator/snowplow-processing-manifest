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
package dynamodb

import scala.collection.convert.decorateAsScala._
import scala.util.Try

import cats.data.ValidatedNel
import cats.implicits._

import io.circe.parser.{parse => jsonParse}

import java.time._
import java.util.UUID
import java.util.regex.Pattern.quote

import com.snowplowanalytics.manifest.core._
import com.snowplowanalytics.manifest.core.ManifestError._

object Serialization {
  import Common._

  /** Transform DynamoDB record into processing manifest record */
  def parse(dbItem: DbItem): Either[ManifestError, Record] = {
    val map = dbItem.asScala

    def getS(key: String): Either[String, String] = for {
      value <- foldO(map.get(key), s"Key $key does not exist")
      str <- foldO(Option(value.getS), s"Key $key is not a string")
    } yield str

    def getPreviousRecordId = (for { value <- map.get(PreviousRecordId); str <- Option(value.getS).map(parseId) } yield str).sequence

    def getT(key: String): Either[String, Instant] = for {
      value <- foldO(map.get(key), s"Key $key does not exist")
      str <- foldO(Option(value.getN), s"Key $key is not a number")
      long <- foldO(Try(str.toLong).toOption, s"Key $key is not an integer")
    } yield Instant.ofEpochMilli(long)

    def getData: Either[String, Option[Payload]] = {
      val data = for {
        value <- map.get(DataKey)
        opt    = Option(value.getS)
        jsonE <- opt.map(jsonParse)
      } yield jsonE.leftMap(_.show).flatMap(Payload.parse)
      data.sequence.leftMap(_ => s"Key $DataKey does not contain valid JSON")
    }

    if (map.isEmpty) { parseError("DynamoDB record does not contain any payload").asLeft } else {
      val id = getS(ItemIdKey).toValidatedNel
      val appState: ValidatedNel[String, (Application, State, UUID)] =
        getS(AppStateKey).toValidatedNel.andThen(v => v.split(quote(Separator.toString), -1).toList match {
          case List(appName, appVersion, instanceId, state, uuid) =>
            (State.parse(state).toValidatedNel, parseId(uuid).toValidatedNel).mapN { (state, recordId) =>
              (Application(Agent(appName, appVersion), expand(instanceId)), state, recordId)
            }
          case _ =>
            val format = s"appName${Separator}appVersion${Separator}instanceId${Separator}state${Separator}uuid"
            s"Value of $AppStateKey key [$v] does not conform $format format".asLeft.toValidatedNel
        })
      val author: ValidatedNel[String, Author] =
        getS(AuthorKey).toValidatedNel.andThen(v => v.split(quote(Separator.toString), -1).toList match {
          case List(name, version, manifestVersion) => Author(Agent(name, version), manifestVersion).validNel
          case _ => s"Value of $AuthorKey key [$v] does not conform expected format".invalidNel
        })
      val timestamp = getT(TimestampKey).toValidatedNel
      val data = getData.toValidatedNel
      val validated = (id, appState, getPreviousRecordId.toValidatedNel, timestamp, author, data).mapN(toRecord)
      validated.toEither.leftMap { errors =>
        parseError(s"Cannot parse manifest record from DynamoDB due following errors: ${errors.toList.mkString(", ")}")
      }
    }
  }

  /** Construct `Record` from just extracted DynamoDB attributes */
  private def toRecord(itemId: String,
                       appState: (Application, State, UUID),
                       previousRecordId: Option[UUID],
                       timestamp: Instant,
                       author: Author,
                       data: Option[Payload]): Record = {
    val (app, state, recordId) = appState
    Record(itemId, app, recordId, previousRecordId, state, timestamp, author, data)
  }

  private[dynamodb] def showAuthor(author: Author): String =
    s"${author.agent.name}$Separator${author.agent.version}$Separator${author.manifestVersion}"

  /** Merge app and state into DynamoDB-compatible single column */
  private[dynamodb] def appState(app: Application, id: UUID, state: State): String =
    s"${showApplication(app)}$Separator${showState(id, state)}"

  def showApplication(app: Application): String =
    s"${app.name}$Separator${app.version}$Separator${app.instanceId.getOrElse("")}"

  def showRecord(record: Record): String =
    s"+ ${showApplication(record.application)} ${showState(record.recordId, record.state)} " +
      s"${record.timestamp} ${showAuthor(record.author)} ${record.payload.map(_.toString).getOrElse("")}"

  def parseId(s: String): Either[String, UUID] =
    try {
      UUID.fromString(s).asRight
    } catch {
      case _: IllegalArgumentException => s"UUID [$s] has invalid format".asLeft
    }

  def showState(id: UUID, state: State): String =
    s"${state.show}$Separator$id"

  private def expand(s: String): Option[String] = if (s.isEmpty) None else Some(s)
}
