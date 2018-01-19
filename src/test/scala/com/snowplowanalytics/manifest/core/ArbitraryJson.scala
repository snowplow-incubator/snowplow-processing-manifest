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

import cats.instances.list._

import io.circe.{
  ArrayEncoder,
  DecodingFailure,
  Json,
  JsonNumber,
  JsonObject,
  KeyDecoder,
  KeyEncoder,
  ObjectEncoder
}
import io.circe.numbers.BiggerDecimal
import org.scalacheck.{ Arbitrary, Cogen, Gen }

object ArbitraryJson {

  /**
    * An arbitrary JSON number, represented as a string.
    */
  final case class JsonNumberString(value: String)

  final object JsonNumberString {
    implicit val arbitraryJsonNumberString: Arbitrary[JsonNumberString] = Arbitrary(
      for {
        sign <- Gen.oneOf("", "-")
        integral <- Gen.oneOf(
          Gen.const("0"),
          for {
            nonZero <- Gen.choose(1, 9).map(_.toString)
            rest <- Gen.numStr
          } yield s"$nonZero$rest"
        )
        fractional <- Gen.oneOf(
          Gen.const(""),
          Gen.nonEmptyListOf(Gen.numChar).map(_.mkString).map("." + _)
        )
        exponent <- Gen.oneOf(
          Gen.const(""),
          for {
            e <- Gen.oneOf("e", "E")
            s <- Gen.oneOf("", "+", "-")
            n <- Gen.nonEmptyListOf(Gen.numChar).map(_.mkString)
          } yield s"$e$s$n"
        )
      } yield JsonNumberString(s"$sign$integral$fractional$exponent")
    )
  }

  /**
    * An integral string with an optional leading minus sign and between 1 and 25
    * digits (inclusive).
    */
  final case class IntegralString(value: String)

  final object IntegralString {
    implicit val arbitraryIntegralString: Arbitrary[IntegralString] = Arbitrary(
      for {
        sign    <- Gen.oneOf("", "-")
        nonZero <- Gen.choose(1, 9).map(_.toString)
        /**
          * We want between 1 and 25 digits, with extra weight on the numbers of
          * digits around the size of `Long.MaxValue`.
          */
        count   <- Gen.chooseNum(0, 24, 17, 18, 19)
        rest    <- Gen.buildableOfN[String, Char](count, Gen.numChar)
      } yield IntegralString(s"$sign$nonZero$rest")
    )
  }

  /**
    * The maximum depth of a generated JSON value.
    */
  protected def maxJsonDepth: Int = 5

  /**
    * The maximum number of values in a generated JSON array.
    */
  protected def maxJsonArraySize: Int = 10

  /**
    * The maximum number of key-value pairs in a generated JSON object.
    */
  protected def maxJsonObjectSize: Int = 10

  protected def withoutMantiss(number: Either[BiggerDecimal, JsonNumber]): Boolean = {
    val s = number.fold(_.toString, _.toString)
    val includes = s.contains("E") || s.contains("e") || s.contains("-0") || s.contains("Infinity")
    val outOfInt = number.fold(_.toLong, _.toLong).forall(n => n > Int.MaxValue || n < Int.MinValue)
    !(includes || outOfInt)
  }

  implicit val arbitraryBiggerDecimal: Arbitrary[BiggerDecimal] = Arbitrary(
    Gen.oneOf(
      Arbitrary.arbitrary[JsonNumberString].map(s => BiggerDecimal.parseBiggerDecimalUnsafe(s.value)),
      Arbitrary.arbitrary[Long].map(BiggerDecimal.fromLong),
      Arbitrary.arbitrary[Double].map(BiggerDecimal.fromDoubleUnsafe),
      Arbitrary.arbitrary[BigInt].map(_.underlying).map(BiggerDecimal.fromBigInteger),
      Arbitrary.arbitrary[BigDecimal].map(_.underlying).map(BiggerDecimal.fromBigDecimal)
    ).suchThat(x => withoutMantiss(Left(x)))
  )

  implicit val arbitraryJsonNumber: Arbitrary[JsonNumber] = Arbitrary(
    Gen.oneOf(
      Arbitrary.arbitrary[IntegralString].map(input => JsonNumber.fromDecimalStringUnsafe(input.value)),
      Arbitrary.arbitrary[JsonNumberString].map(input => JsonNumber.fromDecimalStringUnsafe(input.value)),
      Arbitrary.arbitrary[BigDecimal].map(Json.fromBigDecimal(_).asNumber.get),
      Arbitrary.arbitrary[BigInt].map(Json.fromBigInt(_).asNumber.get),
      Arbitrary.arbitrary[Long].map(Json.fromLong(_).asNumber.get),
      Arbitrary.arbitrary[Double].map(Json.fromDoubleOrString(_).asNumber.get),
      Arbitrary.arbitrary[Float].map(Json.fromFloatOrString(_).asNumber.get)
    ).suchThat(x => withoutMantiss(Right(x)))
  )

  private[this] val genNull: Gen[Json] = Gen.const(Json.Null)
  private[this] val genBool: Gen[Json] = Arbitrary.arbitrary[Boolean].map(Json.fromBoolean)
  private[this] val genString: Gen[Json] = Gen.alphaNumStr.map(Json.fromString)
  private[this] val genNumber: Gen[Json] = Arbitrary.arbitrary[JsonNumber].map(Json.fromJsonNumber)

  private[this] def genArray(depth: Int): Gen[Json] = Gen.choose(0, maxJsonArraySize).flatMap { size =>
    Gen.listOfN(size, genJsonAtDepth(depth + 1)).map(Json.arr)
  }

  private[this] def genJsonObject(depth: Int): Gen[JsonObject] = Gen.choose(0, maxJsonObjectSize).flatMap { size =>
    val fields = Gen.listOfN(
      size,
      for {
        key <- Gen.alphaNumStr
        value <- genJsonAtDepth(depth + 1)
      } yield key -> value
    )

    Gen.oneOf(
      fields.map(JsonObject.fromIterable),
      fields.map(JsonObject.fromFoldable[List])
    )
  }

  private[this] def genJsonAtDepth(depth: Int): Gen[Json] = {
    val genJsons = List(genNumber, genString) ++ (
      if (depth < maxJsonDepth) List(genArray(depth), genJsonObject(depth).map(Json.fromJsonObject)) else Nil
      )

    Gen.oneOf(genNull, genBool, genJsons: _*)
  }

  implicit val arbitraryJson: Arbitrary[Json] = Arbitrary(genJsonAtDepth(0))
  implicit val arbitraryJsonObject: Arbitrary[JsonObject] = Arbitrary(genJsonObject(0))

  implicit val arbitraryDecodingFailure: Arbitrary[DecodingFailure] = Arbitrary(
    Arbitrary.arbitrary[String].map(DecodingFailure(_, Nil))
  )

  implicit def arbitraryKeyEncoder[A: Cogen]: Arbitrary[KeyEncoder[A]] = Arbitrary(
    Arbitrary.arbitrary[A => String].map(KeyEncoder.instance)
  )

  implicit def arbitraryKeyDecoder[A: Arbitrary]: Arbitrary[KeyDecoder[A]] = Arbitrary(
    Arbitrary.arbitrary[String => Option[A]].map(KeyDecoder.instance)
  )

  implicit def arbitraryObjectEncoder[A: Cogen]: Arbitrary[ObjectEncoder[A]] = Arbitrary(
    Arbitrary.arbitrary[A => JsonObject].map(ObjectEncoder.instance)
  )

  implicit def arbitraryArrayEncoder[A: Cogen]: Arbitrary[ArrayEncoder[A]] = Arbitrary(
    Arbitrary.arbitrary[A => Vector[Json]].map(ArrayEncoder.instance)
  )
}
