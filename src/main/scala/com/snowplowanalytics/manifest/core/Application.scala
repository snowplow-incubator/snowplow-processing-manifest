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

import cats.Show

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

/**
  * *Processing* application that added `Record` to manifest
  * Unlike `Author` this can be added on behalf of other application
  * @param agent information about version and number
  * @param instanceId optional app's arguments such as storage target id,
  *             allows two identical applications process same `Item`.
  *             Absent instance id is equal to wildcard
  */
final case class Application(agent: Agent, instanceId: Option[String]) {
  def name: String = agent.name
  def version: String = agent.version

  /**
    * Decide if some application (requester) is allowed to process Item with SKIPPED record
    * This application should be retrieved from `Record.application` with SKIPPED record
    */
  def canBeProcessedBy(requester: Application): Boolean = {
    if (name == "*") { false }      // Nothing can process it
    else { instanceId match {
      case None if requester.name == name => false
      case Some(id) if requester.name == name => !requester.instanceId.contains(id)
      case _ => true
    } }
  }
}

object Application {

  def apply(name: String, version: String): Application =
    Application(Agent(name, version), None)

  implicit final val applicationJsonEncoder: Encoder[Application] =
    Encoder.instance[Application] { application =>
      Json.obj(
        "agent" -> application.agent.asJson,
        "instanceId" -> application.instanceId.fold(Json.Null)(Json.fromString)
      )
    }

  implicit final val applicationJsonDecoder: Decoder[Application] =
    deriveDecoder[Application]

  implicit final val manifestErrorShow: Show[Application] =
    new Show[Application] {
      def show(t: Application): String =
        s"${t.agent.name}:${t.agent.version}:${t.instanceId}"
    }
}
