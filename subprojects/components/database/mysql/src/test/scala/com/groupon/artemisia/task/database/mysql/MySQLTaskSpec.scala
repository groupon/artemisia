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

package com.groupon.artemisia.task.database.mysql

import java.io.File
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.database.{BasicExportSetting, BasicLoadSetting, TestDBInterFactory}
import com.groupon.artemisia.task.hadoop.{HDFSReadSetting, HDFSWriteSetting}
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.FileSystemUtil
import scala.io.Source

/**
  * Created by chlr on 6/2/16.
  */
class MySQLTaskSpec extends TestSpec {

  "MySQLTask" must "execute dml queries correctly" in {
    val table = "mysql_dummy_table1"
    val taskName = "SQLExecuteTest"
    val sqlExecute = new SQLExecute(taskName, s"delete from $table" ,DBConnection("","","","",10)) {
        override val dbInterface = TestDBInterFactory.withDefaultDataLoader(table,mode = Some("mysql"))
    }
    val result = sqlExecute.execute()
    result.getInt(s"$taskName.__stats__.rows-effected") must be (2)
  }

  it must "export query ouput" in {
    val table = "mysql_dummy_table2"
    val taskName = "SQLExportTest"
    FileSystemUtil.withTempFile(fileName = table) {
      file => {
        val task = new ExportToFile(taskName, s"select * from $table", file.toURI ,DBConnection("","","","",10), BasicExportSetting()) {
          override val dbInterface = TestDBInterFactory.withDefaultDataLoader(table,mode = Some("mysql"))
        }
        val result = task.execute()
        task.supportedModes must be === "default" :: "bulk" :: Nil
        result.getInt(s"$taskName.__stats__.rows") must be (2)
        Source.fromFile(file).getLines().toList.head must be ("1,foo,TRUE,100,10000000,87.30,12:30:00,1945-05-09,1945-05-09 12:30:00.0")
      }
    }
  }

  it must "read sql query and emit config value" in {
    val table = "mysql_dummy_table3"
    val taskName = "SQLReadTest"
    val sqlRead = new SQLRead(taskName, s"select col1 from $table where col2 = 'foo'" ,DBConnection("","","","",10)) {
      override val dbInterface = TestDBInterFactory.withDefaultDataLoader(table,mode = Some("mysql"))
    }
    val result = sqlRead.execute()
    result.getInt("COL1") must be (1)
  }

  it must "construct load task" in {
    val task = new LoadFromFile("LoadFromFileTest", "mysql_dummy_table4",
      new File(this.getClass.getResource("/dummy_load_file.txt").getFile).toURI,
      DBConnection("","","","",-1), BasicLoadSetting())
    task.supportedModes must be === "default" :: "bulk" :: Nil
  }

  it must "contruct load hdfs task" in {
    val tableName = "mysql_dummy_table5"
    val taskName = "LoadFromHDFSTest"
    val hdfsReadSetting = HDFSReadSetting(new File(this.getClass.getResource("/dummy_load_file.txt").getFile).toURI)
    val task = new LoadFromHDFS(taskName, tableName, hdfsReadSetting,
      DBConnection("","","","",-1), BasicLoadSetting())
    task.supportedModes must be === "default" :: "bulk" :: Nil
  }

  it must "construct export to hdfs task" in {
    val hdfsWriteSetting = HDFSWriteSetting(new File(this.getClass.getResource("/dummy_load_file.txt").getFile).toURI)
    val task = new ExportToHDFS("SQLExportHDFSTest", "select * from table", hdfsWriteSetting,
      DBConnection("","","","",-1), BasicExportSetting())
    task.supportedModes must be === "default" :: "bulk" :: Nil
  }

}
