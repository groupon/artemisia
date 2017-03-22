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

import java.io._
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}
import com.groupon.artemisia.TestSpec

/**
  * Created by chlr on 7/17/16.
  */

@DoNotDiscover
trait HDFSUtilSpec extends TestSpec with BeforeAndAfterAll {


  "HDFSUtil" must "it must expand uri to file list" in {
   val uri = BaseHDFSSpec.cluster.pathToURI("/test/dir*/*.txt")
   val list = HDFSUtil.expandPath(uri)
    list must have length 4
  }

  it must "merge multiple files paths into a single stream" in {
    val uri = BaseHDFSSpec.cluster.pathToURI("/test/dir*/*.txt")
    val list = HDFSUtil.expandPath(uri)
    val stream = HDFSUtil.mergeFileIOStreams(list)
    val buffered = new BufferedReader(new InputStreamReader(stream))
    Stream.continually(buffered.readLine()).takeWhile( _ != null).toArray must have length 8
    buffered.close()
  }

  it must "read files in hdfs filesystem" in {
    val uri =  BaseHDFSSpec.cluster.pathToURI("/test/dir1/file1.txt")
    val stream = new BufferedReader(new InputStreamReader(HDFSUtil.readIOStream(uri)))
    stream.readLine() must be ("100,tango,true,100,10000000,87.3,12:30:00,1945-05-09,1945-05-09 12:30:00")
    stream.close()
  }

  it must "write files in hdfs filesystem" in {
    val data = "Hello World"
    val uri =  BaseHDFSSpec.cluster.pathToURI("/test/dir1/file3.txt")
    val stream = HDFSUtil.writeIOStream(uri, true, 1, 2048)
    val buffered = new BufferedWriter(new OutputStreamWriter(stream))
    buffered.write(data)
    buffered.close()
    val reader = new BufferedReader(new InputStreamReader(HDFSUtil.readIOStream(uri)))
    reader.readLine() must be (data)
    reader.close()
  }


  it must "handle compression for both read and write" in {
    val data = "I find your lack of faith, disturbing"
    val uri =  BaseHDFSSpec.cluster.pathToURI("/test/dir1/file4.gz")
    val writeStream = HDFSUtil.writeIOStream(
      uri =uri
    ,overwrite = true
    ,replication = 1
    ,blockSize = 512
    ,codec = Some("bzip2"))
    val writer = new BufferedWriter(new OutputStreamWriter(writeStream))
    writer.write(data)
    writer.close()
    val readStream = HDFSUtil.readIOStream(uri, Some("bzip2"))
    val reader = new BufferedReader(new InputStreamReader(readStream))
    reader.readLine must be (data)
  }



}

