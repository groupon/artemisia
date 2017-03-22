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

package com.groupon.artemisia.task.database.teradata.tpt

import org.apache.commons.io.output.NullOutputStream
import com.groupon.artemisia.TestSpec

/**
  * Created by chlr on 9/27/16.
  */
class TPTStreamLogParserSpec extends TestSpec {

  val sampleTPTLog =
    """
      |Job log: /opt/teradata/client/14.10/tbuild/logs/sandbox.chlr_test2-2548.out
      |Job id is sandbox.chlr_test2-2548, running on pit-dev-owagent1
      |Teradata Parallel Transporter SQL DDL Operator Version 14.10.00.12
      |DDL_OPERATOR: private log not specified
      |DDL_OPERATOR: connecting sessions
      |DDL_OPERATOR: sending SQL requests
      |DDL_OPERATOR: disconnecting sessions
      |DDL_OPERATOR: Total processor time used = '0.14 Second(s)'
      |DDL_OPERATOR: Start : Wed Sep 28 01:47:22 2016
      |DDL_OPERATOR: End   : Wed Sep 28 01:47:29 2016
      |Job step DROP_TABLE completed successfully
      |Teradata Parallel Transporter DataConnector Operator Version 14.10.00.12
      |Teradata Parallel Transporter Stream Operator Version 14.10.00.12
      |tpt_writer: private log not specified
      |tpt_reader: Instance 1 directing private log report to 'dtacop-chlr-24064-1'.
      |tpt_reader: DataConnector Producer operator Instances: 1
      |tpt_reader: ECI operator ID: 'tpt_reader-24064'
      |tpt_reader: Operator instance 1 processing file '/tmp/artemisia/64fb5086-1b5c-46b5-99d9-6fe0bdc2d38e/input.pipe'.
      |tpt_writer: Start-up Rate: UNLIMITED statements per Minute
      |tpt_writer: Operator Command ID for External Command Interface: tpt_writer24063
      |tpt_writer: connecting sessions
      |16/09/28 01:47:35 INFO lzo.GPLNativeCodeLoader: Loaded native gpl library
      |16/09/28 01:47:35 INFO lzo.LzoCodec: Successfully loaded & initialized native-lzo library [hadoop-lzo rev 56566259a76a3cf446ec946a6632268c7304af9f]
      |tpt_reader: TPT19003 TPT Exit code set to 4.
      |tpt_writer: entering Load Phase
      |tpt_writer: Load Statistics for DML Group 1 :
      |tpt_writer: Target Table:  'sandbox.chlr_test2'
      |tpt_writer: Rows Inserted: 995
      |tpt_writer: entering Cleanup Phase
      |tpt_writer: disconnecting sessions
      |tpt_reader: Total files processed: 1.
      |tpt_reader: TPT19229 5 error rows sent to error file /tmp/artemisia/64fb5086-1b5c-46b5-99d9-6fe0bdc2d38e/Artemisia/error.txt
      |tpt_writer: Total processor time used = '0.25 Second(s)'
      |tpt_writer: Start : Wed Sep 28 01:47:32 2016
      |tpt_writer: End   : Wed Sep 28 01:48:27 2016
      |Job step LOAD_TABLE terminated (status 4)
      |Job sandbox.chlr_test2 completed successfully, but with warning(s).
      |Job start: Wed Sep 28 01:47:19 2016
      |Job end:   Wed Sep 28 01:48:27 2016
    """.stripMargin


  "TPTStreamLogParser" must "parse TPT stream output log" in {
      val parser = new TPTStreamLogParser(new NullOutputStream)
      parser.write(sampleTPTLog.getBytes())
      parser.appliedRows must be (995)
      parser.errorFileRows must be (5)
      parser.errorTableRows must be (0)
      parser.toConfig.getInt("loaded") must be (995)
      parser.toConfig.getInt("error-file") must be (5)
      parser.toConfig.getInt("error-table") must be (0)
      parser.toConfig.getInt("rejected") must be (5)
      parser.toConfig.getInt("source") must be (1000)
  }

}
