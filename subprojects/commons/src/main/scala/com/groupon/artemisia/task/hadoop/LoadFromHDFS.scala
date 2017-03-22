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

import com.groupon.artemisia.task.database.LoadFromFile
import com.groupon.artemisia.task.settings.{DBConnection, LoadSetting}

/**
  * Created by chlr on 7/19/16.
  */

abstract class LoadFromHDFS(override val taskName: String, override val tableName: String, val hdfsReadSetting: HDFSReadSetting,
                            override val connectionProfile: DBConnection, override val loadSetting: LoadSetting)
  extends LoadFromFile(taskName, tableName, hdfsReadSetting.location, connectionProfile, loadSetting) {

  override lazy val source = Left(LoadFromHDFS.getInputStream(hdfsReadSetting))

}

object LoadFromHDFS {

  /**
    * get inputStream and size of the load path in bytes.
    * @param hdfsReadSetting hdfs read setting object
    * @return inputstream for the location in HDFSReadSetting
    */
  def getPathForLoad(hdfsReadSetting: HDFSReadSetting) = {
    hdfsReadSetting.cliMode match {
      case true =>
        val cliReader = new HDFSCLIReader(hdfsReadSetting.cliBinary)
        cliReader.readPath(hdfsReadSetting.location) -> cliReader.getPathSize(hdfsReadSetting.location)
      case false => HDFSUtil.getPathForLoad(hdfsReadSetting.location, hdfsReadSetting.codec)
    }
  }

  /**
    * get inputStream for the provided HDFSReadSetting object.
    * @param hdfsReadSetting hdfs read setting object
    * @return inputstream for the location in HDFSReadSetting
    */
  def getInputStream(hdfsReadSetting: HDFSReadSetting) = {
    hdfsReadSetting.cliMode match {
      case true => HDFSUtil.mergeFileIOStreams(HDFSUtil.expandPath(hdfsReadSetting.location)
        , hdfsReadSetting.codec)
      case false =>
        val reader = new HDFSCLIReader(hdfsReadSetting.getCLIBinaryPath)
        reader.readPath(hdfsReadSetting.location.toString)
    }
  }

}




