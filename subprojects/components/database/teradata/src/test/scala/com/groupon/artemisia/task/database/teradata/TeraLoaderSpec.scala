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

package com.groupon.artemisia.task.database.teradata

import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.net.URI
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.database.{BasicExportSetting, TestDBInterFactory}
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.FileSystemUtil.{FileEnhancer, withTempFile}
/**
  * Created by chlr on 7/12/16.
  */
class TeraLoaderSpec extends TestSpec {

  "TeradataComponent" must "load data to table" in {
    val tableName = "td_load_test"
    withTempFile(fileName = "td_load_test") {
      file =>
        file <<=
          """ |100,tango,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
              |101,bravo,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
              |102,whiskey,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
              |103,blimey,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00
              |104,victor,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00 """.stripMargin

          val loader = new LoadFromFile(taskName = "td_load_test",tableName = tableName,location = file.toURI,
            connectionProfile = DBConnection("", "", "", "", -1),
            loadSetting = TeraLoadSetting()) {
            override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
            override val source: Either[InputStream, URI] = Left(new BufferedInputStream(new FileInputStream(file)))
          }
        loader.supportedModes must be === "fastload" :: "default" :: "auto" :: Nil
       val result = loader.execute()
        result.getInt("td_load_test.__stats__.loaded") must be (5)
    }
  }

  it must "export data to file" in {
    val tableName = "td_export_test"
    withTempFile(fileName = tableName) {
      file =>
        val export = new ExportToFile(name = tableName, sql = s"SELECT * FROM $tableName", file.toURI
          , connectionProfile = DBConnection("", "", "", "", -1),
          exportSetting = BasicExportSetting()) {
          override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
        }
        export.supportedModes must be === "default" :: "fastexport"  :: Nil
        val result = export.execute()
        result.getInt("td_export_test.__stats__.rows") must be (2)
    }
  }


  it must "execute DML statement" in {
    val tableName = "td_execute"
    val execute = new SQLExecute(name = tableName, sql = s"DELETE FROM $tableName", connectionProfile = DBConnection("", "", "", "", -1)) {
      override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = execute.execute()
    result.getInt("td_execute.__stats__.rows-effected") must be (2)
  }

  it must "run SQLRead task" in {
    val tableName = "td_read"
    val execute = new SQLRead(name = tableName, sql = s"SELECT col2 FROM $tableName WHERE col1 = 1",
      connectionProfile = DBConnection("", "", "", "", -1)) {
      override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = execute.execute()
    result.getString("COL2") must be ("foo")
  }


}
