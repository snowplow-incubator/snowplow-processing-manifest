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

import com.fasterxml.jackson.databind.ObjectMapper

import cats.implicits._

import io.circe.{Json, JsonObject}
import io.circe.parser.parse

import org.specs2.{ScalaCheck, Specification}
import org.scalacheck.Prop

class PayloadSpec extends Specification with ScalaCheck { def is = s2"""
  Correctly transform any Circe AST to Jackson and back $e1
  Correctly transform any Jackson AST to Circe and back $e2
  Get correct iglu validation error $e3
  Get correct iglu not-found error $e4
  """

  def e1 = {
    val objectMapper = new ObjectMapper()

    Prop.forAll(ArbitraryJson.arbitraryJsonObject.arbitrary) {
      (jsonObject: JsonObject) =>
        val json = Json.fromJsonObject(jsonObject)
        val jsonNode = Payload.circeToJackson(json)
        val jsonString = objectMapper.writeValueAsString(jsonNode)

        parse(json.noSpaces) === parse(jsonString)
    }
  }

  def e2 = {
    val objectMapper = new ObjectMapper()

    Prop.forAll(ArbitraryJson.arbitraryJsonObject.arbitrary) {
      (jsonObject: JsonObject) =>
        val json = Json.fromJsonObject(jsonObject)
        val jsonNode = objectMapper.readTree(json.noSpaces)
        val transformed = Payload.jacksonToCirce(jsonNode)

        if (json != transformed) { println(s"JSON: $json"); println(s"TRAN: $transformed")} else ()

        json === transformed
    }
  }

  def e3 = {
    import com.snowplowanalytics.iglu.core.circe.implicits._

    // Turns out FGE JSON Schema validator does not aggregate validation errors
    val selfDescribingJson = parse(
      """
        |{
        |  "schema": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0",
        |  "data": {"latitude": 91}
        |}
      """.stripMargin).right.toOption

    val expected =
        """Iglu error for iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0: """ ++
        """{"level":"error",""" ++
        """"schema":{"loadingURI":"#","pointer":""},""" ++
        """"instance":{"pointer":""},""" ++
        """"domain":"validation",""" ++
        """"keyword":"required",""" ++
        """"message":"object has missing required properties ([\"longitude\"])",""" ++
        """"required":["latitude","longitude"],""" ++
        """"missing":["longitude"]}"""

    Payload.validate(SpecHelpers.igluCentralResolver, selfDescribingJson.flatMap(_.toData).get) must beLeft(expected)
  }

  def e4 = {
    import com.snowplowanalytics.iglu.core.circe.implicits._

    val selfDescribingJson = parse(
      """
        |{
        |  "schema": "iglu:com.snowplowanalytics.snowplow/nonexistent/jsonschema/1-0-0",
        |  "data": {"latitude": 91}
        |}
      """.stripMargin).right.toOption

    val expected =
      """Iglu error for iglu:com.snowplowanalytics.snowplow/nonexistent/jsonschema/1-0-0: """ ++
        """{"level":"error",""" ++
        """"message":"Could not find schema with key iglu:com.snowplowanalytics.snowplow/nonexistent/jsonschema/1-0-0 in any repository, """ ++
        """tried:","repositories":["Iglu Client Embedded [embedded]","Iglu Central [HTTP]"]}"""

    Payload.validate(SpecHelpers.igluCentralResolver, selfDescribingJson.flatMap(_.toData).get) must beLeft(expected)
  }
}
