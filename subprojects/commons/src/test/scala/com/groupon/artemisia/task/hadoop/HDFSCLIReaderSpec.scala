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

import java.io.{BufferedReader, File, InputStreamReader}
import java.net.URI
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.database.{BasicLoadSetting, TestDBInterFactory}
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.TestUtils._

/**
  * Created by chlr on 8/24/16.
  */
class HDFSCLIReaderSpec extends TestSpec {

  "HDFSCLIReader" must "read an HDFS file" in {
    runOnPosix {
      val binary = this.getClass.getResource("/executable/hdfs").getFile
      new File(binary).setExecutable(true)
      val cliReader = new HDFSCLIReader(binary)
      val buffer = new BufferedReader(new InputStreamReader(cliReader.readPath(new URI("hdfs://namenode/path"))))
      val result = Stream.continually(buffer.readLine()).takeWhile(x => x != null)
      result.head must be("100,tango,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00")
      result.length must be(4)
    }
  }

  it must "compute data volume in bytes" in {
    runOnPosix {
      val binary = this.getClass.getResource("/executable/hdfs").getFile
      new File(binary).setExecutable(true)
      val cliReader = new HDFSCLIReader(binary)
      val size = cliReader.getPathSize(new URI("hdfs://namenode/path"))
      size must be (1983515934)
    }
  }

  it must "work with LoadFromHDFS Task" in {
    runOnPosix {
      val tableName = "mysqlLoadFromHDFS"
      val binary = this.getClass.getResource("/executable/hdfs").getFile
      new File(binary).setExecutable(true)
      val task = new LoadFromHDFS(tableName, tableName, HDFSReadSetting(new URI("hdfs://namenode/path"))
        , DBConnection("", "", "", "", -1), BasicLoadSetting()) {
        override val dbInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
        override lazy val source = Left(new HDFSCLIReader(binary).readPath(hdfsReadSetting.location))
        override protected val supportedModes: Seq[String] = "default" :: Nil
      }
      val result = task.execute()
      result.getInt(s"$tableName.__stats__.loaded") must be(4)
      result.getInt(s"$tableName.__stats__.rejected") must be(0)
    }
  }

}
