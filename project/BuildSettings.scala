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
// SBT
import sbt._
import Keys._

// Bintray
import bintray.BintrayPlugin._
import bintray.BintrayKeys._

/**
 * Common settings-patterns for Snowplow apps and libraries.
 * To enable any of these you need to explicitly add Settings value to build.sbt
 */
object BuildSettings {

  // Makes package (build) metadata available withing source code
  lazy val scalifySettings = Seq(
    sourceGenerators in Compile += Def.task {
      val file = (sourceManaged in Compile).value / "settings.scala"
    IO.write(file, """package com.snowplowanalytics.manifest.generated
                      |object ProjectMetadata {
                      |  val version = "%s"
                      |  val name = "%s"
                      |  val organization = "%s"
                      |  val scalaVersion = "%s"
                      |}
                      |""".stripMargin.format(version.value, name.value, organization.value, scalaVersion.value))
      Seq(file)
    }.taskValue
  )

  lazy val buildSettings = Seq[Setting[_]](
    scalacOptions := Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked",
      "-Ywarn-dead-code",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused",
      "-Ywarn-value-discard",
      "-Ypartial-unification",
      "-language:higherKinds",
      "-language:existentials"            // Existential types (besides wildcard types) can be written and inferred
    ),
    javacOptions := Seq(
      "-source", "1.8",
      "-target", "1.8",
      "-Xlint"
    ),
    addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.7")
  )

  lazy val helpersSettings = Seq[Setting[_]](
    // Disable parallel test execution for setting up DynamoDB
    testOptions in Test += Tests.Argument("sequential"),
    initialCommands := "import com.snowplowanalytics.manifest._"
  )

  // Maven Central publishing settings
  lazy val publishSettings = bintraySettings ++ Seq[Setting[_]](
    bintrayOrganization := Some("snowplow"),
    bintrayRepository := "snowplow-maven",
    publishMavenStyle := true,
    publishArtifact := true,
    publishArtifact in Test := false,
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),
    pomIncludeRepository := { _ => false },
    homepage := Some(url("http://snowplowanalytics.com")),
    scmInfo := Some(ScmInfo(url("https://github.com/snowplow-incubator/snowplow-processing-manifest"), "scm:git@github.com:snowplow-incubator/snowplow-processing-manifest.git")),
    pomExtra := (
      <developers>
        <developer>
          <name>Snowplow Analytics Ltd</name>
          <email>support@snowplowanalytics.com</email>
          <organization>Snowplow Analytics Ltd</organization>
          <organizationUrl>http://snowplowanalytics.com</organizationUrl>
        </developer>
      </developers>)
  )
}
