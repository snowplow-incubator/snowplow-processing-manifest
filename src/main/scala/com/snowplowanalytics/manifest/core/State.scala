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

import cats.{Order, Show}
import cats.implicits._

/** Item-application state */
sealed trait State extends Product with Serializable

object State {
  /**
    * New `Item` discovered/added, no actions taken yet
    * Next step is either `Processing` or `Skipped`
    */
  final case object New extends State

  /**
    * Lock acquired on `Item` (released if has joined `Processed`)
    * Should never be set manually by external application (or operator)
    * Next step is either `Processed`, `Failed` or `Skipped`
    */
  final case object Processing extends State

  /**
    * Lock released on `Item` (joined `id` refers to `Processing`)
    * Should never be set manually by external application (or operator)
    * Usually final step for particular application,
    * but next application can also set `Processing` or `Skipped`
    */
  final case object Processed extends State

  /**
    * Operator's attention required (skip if has joined `Resolved`)
    * Should be never set manually by external application (or operator)
    */
  final case object Failed extends State

  /**
    * Operator is acknowledged (joined `id` refers to `Failed`)
    * Applications can resume. Previous step is `Failed`, next step is `Processing`
    */
  final case object Resolved extends State

  /**
    * Forget about `Item`. Has a priority over other steps, therefore
    * if added - it will always be last. Can be added after any step,
    * "closing" an `Item`
    * Cannot be added by processing application, only by operational
    */
  final case object Skipped extends State

  final def parse(s: String): Either[String, State] = s match {
    case "NEW" => New.asRight
    case "PROCESSING" => Processing.asRight
    case "PROCESSED" => Processed.asRight
    case "FAILED" => Failed.asRight
    case "RESOLVED" => Resolved.asRight
    case "SKIPPED" => Skipped.asRight
    case _ => s"Value [$s] is not valid manifest State representation".asLeft
  }

  implicit final val stateShow: Show[State] = new Show[State] {
    def show(t: State): String = t.toString.toUpperCase
  }

  val ordered = List(New, Processing, Failed, Resolved, Processed, Skipped)

  // Very arbitrary defined order. Should be at least `PartialOrder`
  implicit final val order: Order[State] = new Order[State] {
    def compare(x: State, y: State): Int = {
      val ix = ordered.indexOf(x)
      val iy = ordered.indexOf(y)
      if (ix == iy) { 0 } else if (ix < iy) { -1 } else { -1 }
    }
  }

  implicit final val stateJsonEncoder: Encoder[State] =
    Encoder.instance[State](step => Json.fromString(step.show))

  implicit final val stateJsonDecoder: Decoder[State] =
    Decoder[String].emap(parse)
}

