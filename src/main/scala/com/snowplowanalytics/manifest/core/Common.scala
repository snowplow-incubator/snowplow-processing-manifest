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

import cats.implicits._

import io.circe._

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.Registry

/** Helper functions */
object Common {

  private val resolverRefConf = Registry.Config("Processing Manifest Embedded", 0, List("com.snowplowanalytics"))

  /** Registry embedded into Processing Manifest jar */
  val EmbeddedRegistry = Registry.Embedded(resolverRefConf, "/com.snowplowanalytics.manifest/embedded-registry")

  /** Iglu Resolver containing all schemas processing manifest uses */
  val DefaultResolver = Resolver(List(EmbeddedRegistry), None)    // TODO: F

  def decodeKey[A: Decoder](map: Map[String, Json], cursor: HCursor)(key: String): Decoder.Result[A] =
    map.get(key).map(_.as[A]) match {
      case Some(result) => result
      case None => DecodingFailure(s"Property $key is missing", cursor.history).asLeft
    }
}
