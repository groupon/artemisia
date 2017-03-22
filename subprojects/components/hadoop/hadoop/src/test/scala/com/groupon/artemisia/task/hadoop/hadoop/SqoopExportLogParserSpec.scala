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

import java.io.ByteArrayOutputStream
import com.groupon.artemisia.TestSpec

/**
  * Created by chlr on 10/20/16.
  */
class SqoopExportLogParserSpec extends TestSpec {


  "SqoopExportLogParser" must "parse the output log and gether stats" in {

    val content =
      """
        |16/10/21 01:51:50 INFO mapred.JobClient:   Map-Reduce Framework
        |16/10/21 01:51:50 INFO mapred.JobClient:     Map input records=8
        |16/10/21 01:51:50 INFO mapred.JobClient:     Map output records=8
        |16/10/21 01:51:50 INFO mapred.JobClient:     Input split bytes=87
        |16/10/21 01:51:50 INFO mapred.JobClient:     Spilled Records=0
        |16/10/21 01:51:50 INFO mapred.JobClient:     CPU time spent (ms)=1110
      """.stripMargin
    val stream = new ByteArrayOutputStream()
    val parser = new SqoopExportLogParser(stream)
    parser.write(content.getBytes)
    parser.close()
    parser.inputRecords must be (8)
    parser.outputRecords must be (8)
  }


}
