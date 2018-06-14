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
lazy val root = project.in(file("."))
  .settings(
    name := "snowplow-processing-manifest",
    autoStartServer := false,
    version := "0.1.0-M5",
    organization := "com.snowplowanalytics",
    scalaVersion := "2.11.12",
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0"))
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(BuildSettings.publishSettings)
  .settings(
    resolvers ++= Seq(
      "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
    ),
    libraryDependencies ++= Seq(
      Dependencies.circe,
      Dependencies.circeGeneric,
      Dependencies.circeParser,
      Dependencies.circeJavaTime,
      Dependencies.dynamodb,
      Dependencies.cats,
      Dependencies.fs2,
      Dependencies.igluClient,
      Dependencies.igluCore,
      Dependencies.igluCoreCirce,

      Dependencies.specs2,
      Dependencies.specs2Scalacheck,
      Dependencies.scalaCheck
    )
  )
  .settings(BuildSettings.helpersSettings)

