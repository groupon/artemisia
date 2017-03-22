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

import java.io.File
import java.net.URI
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.hadoop.test.PathUtils

/**
  * Created by chlr on 7/22/16.
  */

class TestHDFSCluster(cluster: String) {

  var dfs: MiniDFSCluster = _
  val testDataPath = new File(PathUtils.getTestDir(this.getClass),cluster)
  var conf: HdfsConfiguration = _
  setup()

  private def setup() = {
    conf = new HdfsConfiguration()
    val testDataCluster1 = new File(testDataPath, cluster)
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDataCluster1.getAbsolutePath)
    conf.setInt("dfs.blocksize", 512)
    conf.setInt("dfs.namenode.fs-limits.min-block-size", 512)
    dfs =  new MiniDFSCluster.Builder(conf).build()
    dfs.waitActive()
  }

  def initialize(sourceDir: String, hdfsDir: String = "/test") = {
    val fileSystem: org.apache.hadoop.fs.FileSystem = dfs.getFileSystem
    fileSystem.copyFromLocalFile(false, new Path(sourceDir),
      new Path(hdfsDir))
  }


  def pathToURI(path: String) = {
    new URI(s"hdfs://localhost:${dfs.getNameNodePort}$path")
  }

  def terminate() = {
    val dataDir = new Path(testDataPath.getParentFile.getParentFile.getParent)
    dfs.getFileSystem.delete(dataDir, true)
    val rootTestFile = new File(testDataPath.getParentFile.getParentFile.getParent)
    val rootTestDir = rootTestFile.getAbsolutePath
    val rootTestPath = new Path(rootTestDir)
    val localFileSystem = FileSystem.getLocal(conf)
    localFileSystem.delete(rootTestPath, true)
    dfs.shutdown()
  }



}
