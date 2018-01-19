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

import org.specs2.Specification

class ApplicationSpec extends Specification { def is = s2"""
  Application is blocked by wild-card app $e1
  Application is not blocked by another app $e2
  Application is not blocked by same app with id is different $e3
  Application is not blocked by another app with same id $e4
  """

  def e1 = {
    val fromRecord = Application(Agent("*", "0.1.0"), None)
    val requester = Application(Agent("rdb-loader", "1.1.0"), None)
    fromRecord.canBeProcessedBy(requester) must beFalse
  }

  def e2 = {
    val fromRecord = Application(Agent("any-app", "0.1.0"), None)
    val requester = Application(Agent("rdb-loader", "1.1.0"), None)
    fromRecord.canBeProcessedBy(requester) must beTrue
  }

  def e3 = {
    val fromRecord = Application(Agent("rdb-loader", "0.1.0"), Some("id1"))
    val requester = Application(Agent("rdb-loader", "0.1.0"), Some("id2"))
    fromRecord.canBeProcessedBy(requester) must beTrue
  }

  def e4 = {
    val fromRecord = Application(Agent("any-app", "0.1.0"), Some("id1"))
    val requester = Application(Agent("rdb-loader", "0.1.0"), Some("id1"))
    fromRecord.canBeProcessedBy(requester) must beTrue
  }
}
