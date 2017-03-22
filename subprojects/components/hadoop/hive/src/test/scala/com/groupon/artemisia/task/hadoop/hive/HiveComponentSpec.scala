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

package com.groupon.artemisia.task.hadoop.hive

import com.typesafe.config.ConfigFactory
import com.groupon.artemisia.TestSpec

/**
  * Created by chlr on 8/7/16.
  */
class HiveComponentSpec extends TestSpec {

  val component = new HiveComponent("Hive")

  "HiveComponent" must "dispatch HQLExecute when requested" in {
    val config = ConfigFactory parseString
      s"""
         | {
         |  sql = "select * from table"
         | }
       """.stripMargin
    val task = component.dispatchTask("HQLExecute", "task", config).asInstanceOf[HQLExecute]
    task.sql must be ("select * from table")
  }

  it must "dispatch HQLRead when requested" in {
    val config = ConfigFactory parseString
      s"""
         | {
         |  dsn = {
         |    host = hiveserver
         |    username = ironman
         |    password = stark
         |    database = db
         |    port = 1000
         |  }
         |  sql = "select * from table"
         | }
       """.stripMargin
    val task = component.dispatchTask("HQLRead", "task", config).asInstanceOf[HQLRead]
    task.sql must be ("select * from table")
    task.connectionProfile.hostname must be ("hiveserver")
    task.connectionProfile.password must be ("stark")
  }

  it must "dispatch HQLExport when requested" in {

    val config = ConfigFactory parseString
      s"""
         | {
         |  dsn = {
         |    host = hiveserver
         |    username = ironman
         |    password = stark
         |    database = db
         |    port = 1000
         |  }
         |  location = ${this.getClass.getResource("/dummy_file.txt").getFile}
         |  sql = "select * from table_export"
         | }
       """.stripMargin
    val task = component.dispatchTask("HQLExport", "task", config).asInstanceOf[HQLExport]
    task.connectionProfile.default_database must be ("db")
    task.connectionProfile.port must be (1000)
    task.sql must be ("select * from table_export")
  }

}
