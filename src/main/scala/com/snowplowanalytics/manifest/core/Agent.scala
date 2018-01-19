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
import io.circe.generic.semiauto._

/** Common entity for applications interacting with manifest */
case class Agent(name: String, version: String)

object Agent {

  implicit final val agentJsonDecoder: Decoder[Agent] =
    deriveDecoder[Agent]

  implicit final val agentJsonEncoder: Encoder[Agent] =
    Encoder.instance[Agent] { agent =>
      Json.obj(
        "name" -> Json.fromString(agent.name),
        "version" -> Json.fromString(agent.version)
      )
    }
}
