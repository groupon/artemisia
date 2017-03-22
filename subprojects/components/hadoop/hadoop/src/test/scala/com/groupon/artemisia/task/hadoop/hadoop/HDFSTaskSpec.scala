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

package com.groupon.artemisia.task.hadoop.hadoop

import com.typesafe.config.ConfigFactory
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.util.TestUtils

/**
  * Created by chlr on 10/22/16.
  */
class HDFSTaskSpec extends TestSpec {

  "HDFSTask" must "construct itself from config" in {
    val config = ConfigFactory parseString {
      """
        |{
        |  hdfs-bin = /usr/local/bin/hdfs
        |  action = copyFromLocal
        |  args = [
        |    "/var/path/local/file.txt"
        |    "/usr/artemisia/hdfs/file.txt"
        |  ]
        |}
      """.stripMargin
    }
    val task = HDFSTask("task", config).asInstanceOf[HDFSTask]
    task.hdfsBin must be (Some("/usr/local/bin/hdfs"))
    task.action must be ("copyFromLocal")
    task.arguments must contain theSameElementsInOrderAs Seq("/var/path/local/file.txt", "/usr/artemisia/hdfs/file.txt")
  }


  it must "generate right hfds command" in {
    val task = new HDFSTask(
      "test",
      "get",
      Seq("/var/file/local_file", "/var/file/remote_file"),
      Some(TestUtils.getExecutable(this.getClass.getResource("/hdfs.sh")))
    ) {
      command must contain theSameElementsInOrderAs Seq(this.getClass.getResource("/hdfs.sh").getFile, "dfs", "-get",
        "/var/file/local_file", "/var/file/remote_file")
    }
    val result = task.execute()
    result.getString("test.__stats__.stderr") must be ("this is stderr\n")
    result.getString("test.__stats__.stdout") must be ("this is stdout\n")
  }
}
