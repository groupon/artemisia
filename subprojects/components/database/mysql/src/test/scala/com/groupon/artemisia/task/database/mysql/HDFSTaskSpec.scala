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

import org.scalatest.BeforeAndAfterAll
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.database.{BasicExportSetting, BasicLoadSetting, TestDBInterFactory}
import com.groupon.artemisia.task.hadoop.{HDFSReadSetting, HDFSWriteSetting, TestHDFSCluster}
import com.groupon.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 7/23/16.
  */
class HDFSTaskSpec extends TestSpec with BeforeAndAfterAll {

  import HDFSTaskSpec._

  override def beforeAll() = {
    cluster.initialize(this.getClass.getResource("/samplefiles").getFile)
  }

  "ExportToHDFS" must "export data to HDFS" in {
    val tableName = "mysqlExportHDFS"
    val task = new ExportToHDFS(tableName, s"SELECT * FROM $tableName",
      HDFSWriteSetting(cluster.pathToURI("/test/dir*/*.txt")), DBConnection("","","","",-1),
    BasicExportSetting()) {
      override val dbInterface =  TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = task.execute()
    result.getInt("mysqlExportHDFS.__stats__.rows") must be (2)
  }

  "LoadFromHDFSHelper" must "load data from HDFS" in {
    val tableName = "mysqlLoadFromHDFS"
    val task = new LoadFromHDFS(tableName, tableName, HDFSReadSetting(cluster.pathToURI("/test/file.txt"))
    , DBConnection("","","","",-1), BasicLoadSetting()) {
      override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = task.execute()
    result.getInt(s"$tableName.__stats__.loaded") must be (2)
    result.getInt(s"$tableName.__stats__.rejected") must be (0)
  }

  override def afterAll() = {
    cluster.terminate()
  }


}

object HDFSTaskSpec {

  val cluster = new TestHDFSCluster("mysql")

}
