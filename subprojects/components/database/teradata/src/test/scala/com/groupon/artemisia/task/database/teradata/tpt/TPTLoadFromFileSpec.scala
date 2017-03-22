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

package com.groupon.artemisia.task.database.teradata.tpt

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.output.NullOutputStream
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.database.TestDBInterFactory
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.TestUtils._
import scala.concurrent.Future
import com.groupon.artemisia.util.FileSystemUtil._

/**
  * Created by chlr on 9/16/16.
  */
class   TPTLoadFromFileSpec extends TestSpec {

  "TPTLoadFromFile" must "construct itself from config" in {
    val location = joinPath(this.getClass.getResource("/samplefiles").getFile,"dir*/file*.txt")
    val config = ConfigFactory parseString
       s"""
         |{
         |   dsn = {
         |      host = server-name
         |      username = username
         |      password = password
         |      database = sandbox
         |      port = 1025
         |   }
         |   load = {
         |      delimiter = ","
         |      quoting = yes
         |      truncate = yes
         |      header = yes
         |      load-attrs = {}
         |      batch-size = 212312
         |      error-limit = 1234
         |      bulk-threshold = 1M
         |      quotechar = "~"
         |      escapechar = "&"
         |      mode = "fastload"
         |      skip-lines = 100
         |      dtconn-attrs = {
         |        MYDATACONNOPTR1 = Chicago
         |        MYDATACONNOPTR2 = {
         |          type = INTEGER
         |          value = Faizahville
         |        }
         |      }
         |      load-attrs = {
         |        MYLOADCONNOPTR1 = Gotham
         |        MYLOADCONNOPTR2 = {
         |         type = INTEGER
         |         value = Metropolis
         |        }
         |      }
         |   }
         |   destination-table = sandbox.chlr_test2
         |   location = "$location"
         | }
       """.stripMargin
    val task = TPTLoadFromFile("test_job", config).asInstanceOf[TPTLoadFromFile]
    task.loadSetting.nullString must be (None)
    task.loadSetting.mode must be ("fastload")
    task.loadSetting.quotechar must be ('~')
    task.loadSetting.batchSize must be (212312)
    task.loadSetting.bulkLoadThreshold must be (1048576L)
    task.loadSetting.escapechar must be ('&')
    task.loadSetting.delimiter must be (',')
    task.loadSetting.errorLimit must be (1234)
    task.loadSetting.dataConnectorAttrs must be (Map(
      "MYDATACONNOPTR1" -> ("VARCHAR","Chicago")
      ,"MYDATACONNOPTR2" -> ("INTEGER", "Faizahville")
    ))
    task.loadSetting.loadOperatorAttrs must be (Map(
      "MYLOADCONNOPTR1" -> ("VARCHAR", "Gotham"),
      "MYLOADCONNOPTR2" -> ("INTEGER", "Metropolis")
      )
    )
    task.connectionProfile.default_database must be ("sandbox")
    task.connectionProfile.hostname must be ("server-name")
    task.tableName must be ("sandbox.chlr_test2")
    task.location.toString must be (location)
  }

  it must "execute the load task" in {
    val task = new TPTLoadFromFile("test_load"
      ,"test_table"
      ,this.getClass.getResource("/samplefiles/file.txt").toURI
      ,DBConnection.getDummyConnection
      ,TPTLoadSetting()) {
      override protected val loadDataSize: Long = 10L
      override implicit val dbInterface = TestDBInterFactory.withDefaultDataLoader("test_table")
      override lazy val tbuildBin = getExecutable(this.getClass.getResource("/executables/tbuild_load_from_file.sh"))
      override lazy val twbKillBin = getExecutable(this.getClass.getResource("/executables/nop_execute.sh"))
      override lazy val twbStat = getExecutable(this.getClass.getResource("/executables/nop_execute.sh"))
      override val logParser = new TPTLoadLogParser(new NullOutputStream())
      override lazy val readerFuture = Future.successful(())
      override val scriptGenerator = new TPTFastLoadScrGen(
        TPTLoadConfig("database", "table", "/var/path", "input.pipe"),
        TPTLoadSetting(dataConnectorAttrs = Map("ROWERRFILENAME" -> ("VARCHAR","/var/path/errorfile"))),
        DBConnection("td_server", "voltron", "password", "dbc", 1025)
      ) {
        override lazy val dbInterface = ???
        override lazy val tableMetadata = Seq(
          ("col1", "I1", 25: Short, "col1_1", "N"),
          ("col2", "I1", 25: Short, "col2_2", "Y")
        )
      }
    }
    val result = task.execute()
    result.getInt("test_load.__stats__.loaded") must be (1000000)
    result.getInt("test_load.__stats__.sent") must be (1000000)
    result.getInt("test_load.__stats__.duplicate") must be (0)
  }


}
