/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

import sbt.Keys._
import sbt._

object General {

  val group_id = "com.groupon.artemisia"
  val mainScalaVersion = "2.11.7"
  val appVersion = "0.1-SNAPSHOT"
  val subprojectBase = file("subprojects")
  val componentBase = subprojectBase / "components"
  val dependencies = Seq(
    "ch.qos.logback" % "logback-classic" % "0.9.28",
    "ch.qos.logback" % "logback-classic" % "0.9.28" % "runtime",
    "org.slf4j" % "slf4j-api" % "1.7.6" % "provided",
    "org.slf4j" % "slf4j-nop" % "1.7.6" % "test",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test",
    "org.pegdown" % "pegdown" % "1.0.2" % "test",
    "joda-time" % "joda-time" % "2.0",
    "com.typesafe" % "config" % "1.2.1"
  )
  val crossVersions =  Seq(mainScalaVersion)


  def settings(module: String, publishable: Boolean = true) = Seq(
    name := module,
    organization := General.group_id,
    version := General.appVersion,
    scalaVersion := General.mainScalaVersion,
    libraryDependencies ++= General.dependencies,
    libraryDependencies ++= Dependencies.fetchDependencies(module),
    libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }, // required to remove multiple slf4j bindings.
      crossScalaVersions := General.crossVersions,
    (dependencyClasspath in Test) <<= (dependencyClasspath in Test) map {
      _.filterNot(_.data.name.contains("logback-classic"))
    },
    resolvers += Resolver.jcenterRepo,
    credentials += Credentials(
      "Sonatype Nexus Repository Manager",
      "oss.sonatype.org",
      Option(System.getenv().get("SONATYPE_USERNAME")).getOrElse("NOT FOUND!!!"),
      Option(System.getenv().get("SONATYPE_PASSWORD")).getOrElse("NOT FOUND!!!")
    ),
    publish <<= publish.dependsOn(Def.task[Unit]( assert(!Dependencies.packageMode, "package mode must be set to false")))
  ) ++ (if (publishable) Publish.settings else Seq())


}

