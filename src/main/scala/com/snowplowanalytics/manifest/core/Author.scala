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

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

/**
  * Processing or operational agent that added `Record` to manifest
  * Unlike `Application` this is always real appliction that added record
  * @param agent information about version and number
  * @param manifestVersion version of embedded processing manifest
  */
final case class Author(agent: Agent, manifestVersion: String) {
  def name: String = agent.name

  def version: String = agent.version
}

object Author {

  implicit final val authorJsonEncoder: Encoder[Author] =
    Encoder.instance[Author] { author =>
      Json.obj(
        "agent" -> author.agent.asJson,
        "manifestVersion" -> Json.fromString(author.manifestVersion)
      )
    }

  implicit final val authorJsonDecoder: Decoder[Author] =
    deriveDecoder[Author]
}
