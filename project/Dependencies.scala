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

import sbt._

object Dependencies {

  val HADOOP_VERSION = "2.7.2"
  val HIVE_SERVER_VERSION = "0.10.0-cdh4.2.0"

  val packageMode = true

  implicit class ProvidedDependencyHandler(list: Seq[sbt.ModuleID]) {
    def applyScope = list map {
      case x if packageMode => x
      case x => x % "provided"
    }
  }

  private val dependencies: Map[String, Seq[sbt.ModuleID]] = Map(

    "artemisia" -> Seq(
      "com.github.scopt" %% "scopt" % "3.3.0",
      "com.typesafe.akka" %% "akka-actor" % "2.3.15",
      "org.scalaz" %% "scalaz-core" % "7.2.0",
      "com.typesafe.akka" %% "akka-testkit" % "2.3.15" % "test",
      "com.twitter" %% "util-eval" % "6.33.0",
      "org.yaml" % "snakeyaml" % "1.17"
    ),

    "commons" -> { Seq(
      "com.google.guava" % "guava" % "19.0",
      "com.opencsv" % "opencsv" % "3.7",
      "com.h2database" % "h2" % "1.4.191" % "test",
      "org.apache.commons" % "commons-exec" % "1.3",
      "commons-io" % "commons-io" % "2.5",
      "commons-lang" % "commons-lang" % "2.6",
      "org.apache.hadoop" % "hadoop-hdfs" % HADOOP_VERSION % "test",
      "org.apache.hadoop" % "hadoop-common" % HADOOP_VERSION % "test",
      "org.apache.hadoop" % "hadoop-hdfs" % HADOOP_VERSION % "test" classifier "tests",
      "org.apache.hadoop" % "hadoop-common" % HADOOP_VERSION % "test" classifier "tests"
      ) ++ Seq("org.apache.hadoop" % "hadoop-client" % "2.7.2").applyScope
    },

    "mysql" -> Seq("org.mariadb.jdbc" % "mariadb-java-client" % "1.5.8"),

    "postgres" -> Seq("postgresql" % "postgresql" % "9.1-901-1.jdbc4"),

    "localhost" -> Seq(
      "org.apache.commons" % "commons-email" % "1.2",
      "com.jcraft" % "jsch" % "0.1.53",
      "org.apache.sshd" % "sshd-core" % "1.2.0",
      "com.eclipsesource.minimal-json" % "minimal-json" % "0.9.4",
      "com.squareup.okhttp3" % "okhttp" % "3.5.0"
    ),

    "hive" -> Seq("org.apache.hive" % "hive-jdbc" % HIVE_SERVER_VERSION).applyScope
 )

  def fetchDependencies(project: String): Seq[sbt.ModuleID] = {
    dependencies.get(project) match {
      case Some(x) => x
      case None => Seq()
    }
  }

}
