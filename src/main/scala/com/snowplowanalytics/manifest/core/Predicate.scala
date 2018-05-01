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

sealed trait Predicate {
  def and(other: Predicate): Predicate = Predicate.And(this, other)
}

object Predicate {
  case class NotProcessedBy(application: Application) extends Predicate
  case class ProcessedBy(application: Application) extends Predicate
  case class And(a: Predicate, b: Predicate) extends Predicate

  trait PredicateInterpreter[A] {
    def interpret(predicate: Predicate): A
  }
}