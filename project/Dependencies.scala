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
import sbt._

object Dependencies {

  object V {
    // Java
    val dynamodb         = "1.11.319"
    // Scala
    val circe            = "0.9.3"
    val cats             = "1.1.0"
    val fs2              = "0.10.5"
    val igluClient       = "0.5.0"
    val igluCore         = "0.2.0"
    // Scala (test only)
    val specs2           = "4.0.4"
    val scalaCheck       = "1.13.4"
  }

  // Circe
  val circe            = "io.circe"                   %% "circe-core"           % V.circe
  val circeParser      = "io.circe"                   %% "circe-parser"         % V.circe
  val circeGeneric     = "io.circe"                   %% "circe-generic"        % V.circe
  val circeJavaTime    = "io.circe"                   %% "circe-java8"          % V.circe
  val cats             = "org.typelevel"              %% "cats-core"            % V.cats
  val fs2              = "co.fs2"                     %% "fs2-core"             % V.fs2
  val igluClient       = "com.snowplowanalytics"      %% "iglu-scala-client"    % V.igluClient
  val igluCore         = "com.snowplowanalytics"      %% "iglu-core"            % V.igluCore
  val igluCoreCirce    = "com.snowplowanalytics"      %% "iglu-core-circe"      % V.igluCore
  // Java
  val dynamodb         = "com.amazonaws"              % "aws-java-sdk-dynamodb" % V.dynamodb
  // Scala (test only)
  val specs2           = "org.specs2"                 %% "specs2-core"          % V.specs2         % "test"
  val specs2Scalacheck = "org.specs2"                 %% "specs2-scalacheck"    % V.specs2         % "test"
  val scalaCheck       = "org.scalacheck"             %% "scalacheck"           % V.scalaCheck     % "test"
  val circeTesting     = "io.circe"                   %% "circe-testing"        % V.circe          % "test"
}
