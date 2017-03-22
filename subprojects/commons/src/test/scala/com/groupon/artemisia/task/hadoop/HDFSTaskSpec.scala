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

package com.groupon.artemisia.task.hadoop

import java.io.{BufferedReader, InputStreamReader}
import org.scalatest.DoNotDiscover
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.database.{BasicExportSetting, BasicLoadSetting, DBInterface, TestDBInterFactory}
import com.groupon.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 7/20/16.
  */

@DoNotDiscover
trait HDFSTaskSpec extends TestSpec {


  "ExportToHDFS" must "export data to HDFS" in {

    val tableName = "ExportToHDFSSpec_1"
    val location =  BaseHDFSSpec.cluster.pathToURI("/test/dir2/file100.txt")
     val task = new ExportToHDFS(
        "hdfs-task"
        ,s"SELECT * FROM $tableName"
        ,HDFSWriteSetting(location)
        ,DBConnection("hostname", "username", "password", "database", -1)
        ,BasicExportSetting()
      ) {
        override val supportedModes = "default" :: "bulk" :: Nil
        override val dbInterface: DBInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
      }
    task.execute()
    val stream = new BufferedReader(new InputStreamReader(HDFSUtil.readIOStream(location)))
    stream.readLine() must be ("1,foo,TRUE,100,10000000,87.30,12:30:00,1945-05-09,1945-05-09 12:30:00.0")
    stream.close()
  }

  "LoadFromHDFSHelper" must "load data from HDFS" in {
    val tableName = "LoadFromHDFSSpec_2"
    val location =  BaseHDFSSpec.cluster.pathToURI("/test/dir*/*.txt")
    val task = new LoadFromHDFS(
       "hdfs-task"
      ,tableName
      ,HDFSReadSetting(location)
      ,DBConnection("hostname", "username", "password", "db", -1)
      ,BasicLoadSetting(delimiter=',')
    ) {
      override val supportedModes = "default" :: "bulk" :: Nil
      override val dbInterface: DBInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    }
    val result = task.execute()
    result.getInt("hdfs-task.__stats__.loaded") must be > 5
  }
}
