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

import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.database.{BasicExportSetting, TestDBInterFactory}
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.FileSystemUtil._

/**
  * Created by chlr on 8/7/16.
  */
class HiveServerInterfaceSpec extends TestSpec {

  "HiveServerDBInterface" must "execute query" in {
    val tableName = "hive_table_execute"
    val task = new HQLExecute("hql_execute", s"delete from $tableName", Some(DBConnection.getDummyConnection)) {
      override lazy val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = task.execute()
    result.getInt("hql_execute.__stats__.rows-effected") must be (2)
  }

  it must "export query" in {
    val tableName = "hive_table_export"
    withTempFile(fileName = tableName) {
      file =>
        val task = new HQLExport("hql_execute", s"select * from $tableName", file.toURI
          ,DBConnection.getDummyConnection , BasicExportSetting()) {
          override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
        }
        val result = task.execute()
        result.getInt(s"hql_execute.__stats__.rows") must be (2)
    }
  }

  it must "support sqlread" in {
    val tableName = "hive_table_sqlread"
    val task = new HQLRead("hql_execute", s"select count(*) as cnt from $tableName", Some(DBConnection.getDummyConnection)) {
      override lazy val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = task.execute()
    result.getInt("CNT") must be (2)
  }

 }
