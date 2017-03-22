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

package com.groupon.artemisia.util

import java.io.{BufferedReader, InputStreamReader}
import java.nio.file.Paths
import com.groupon.artemisia.TestSpec
/**
 * Created by chlr on 12/11/15.
 */
class FileSystemUtilSpec extends TestSpec  {

  "FileSystemUtil" must "Join multiple path strings into valid path string" in {
    var path1 = "/var/tmp"
    var path2 = "artemisia"
    var path3 = ""
    FileSystemUtil.joinPath (path1,path2,path3) must be ("/var/tmp/artemisia")
    path1 = "/var/tmp/"
    path2 = "/artemisia"
    FileSystemUtil.joinPath (path1,path2,path3) must be ("/var/tmp/artemisia")
    path1 = "/var/tmp"
    path2 = "/artemisia"
    FileSystemUtil.joinPath (path1,path2,path3) must be ("/var/tmp/artemisia")
    path1 = "var"
    path2 = "tmp"
    path3 = "artemisia"
    FileSystemUtil.joinPath (path1,path2,path3) must be ("/var/tmp/artemisia")
  }

  it must "properly construct URI object" in {
    val path1 = "/var/tmp/dir"
    val path2 = "hdfs://var/tmp/dir2"
    FileSystemUtil.makeURI(path1).toString must be ("file:/var/tmp/dir")
    FileSystemUtil.makeURI(path2).toString must be ("hdfs://var/tmp/dir2")
  }

  it must "it must resolve globs" in {
    val path = this.getClass.getResource("/arbitary/glob").getFile
    val location = FileSystemUtil.joinPath(path ,"**/*.txt")
    val files = FileSystemUtil.expandPathToFiles(Paths.get(location))
    files must have size 4
    for (file <- files) { file.exists() mustBe true }
  }

  it must "reads all contents that points to multiple files via globbed path" in {
    val path = this.getClass.getResource("/arbitary/glob").getFile
    val location = FileSystemUtil.joinPath(path ,"**/*.txt")
    val files = FileSystemUtil.expandPathToFiles(Paths.get(location))
    val reader = new BufferedReader(new InputStreamReader(FileSystemUtil.mergeFileStreams(files)))
    Stream.continually(reader.readLine()).takeWhile(_ != null).toArray must have size 8
  }

}
