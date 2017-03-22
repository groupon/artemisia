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
  * Created by chlr on 9/14/16.
  */
class TPTLoadLogParserSpec extends TestSpec {

  val sampleTPTLog =
    """
      |
      |Teradata Parallel Transporter Version 14.10.00.12
      |Job log: /opt/teradata/client/14.10/tbuild/logs/sandbox.chlr_test2-2477.out
      |Job id is sandbox.chlr_test2-2477, running on pit-dev-owagent1
      |Teradata Parallel Transporter SQL DDL Operator Version 14.10.00.12
      |DDL_OPERATOR: private log not specified
      |DDL_OPERATOR: connecting sessions
      |DDL_OPERATOR: sending SQL requests
      |DDL_OPERATOR: TPT10508: RDBMS error 3807: Object 'sandbox.chlr_test2_WT' does not exist.
      |DDL_OPERATOR: TPT18046: Error is ignored as requested in ErrorList
      |DDL_OPERATOR: TPT10508: RDBMS error 3807: Object 'sandbox.chlr_test2_ET' does not exist.
      |DDL_OPERATOR: TPT18046: Error is ignored as requested in ErrorList
      |DDL_OPERATOR: TPT10508: RDBMS error 3807: Object 'sandbox.chlr_test2_UV' does not exist.
      |DDL_OPERATOR: TPT18046: Error is ignored as requested in ErrorList
      |DDL_OPERATOR: TPT10508: RDBMS error 3807: Object 'sandbox.chlr_test2_LG' does not exist.
      |DDL_OPERATOR: TPT18046: Error is ignored as requested in ErrorList
      |DDL_OPERATOR: disconnecting sessions
      |DDL_OPERATOR: Total processor time used = '0.12 Second(s)'
      |DDL_OPERATOR: Start : Wed Sep 14 19:12:55 2016
      |DDL_OPERATOR: End   : Wed Sep 14 19:13:01 2016
      |Job step DROP_TABLE completed successfully
      |Teradata Parallel Transporter DataConnector Operator Version 14.10.00.12
      |tpt_reader: Instance 1 directing private log report to 'dtacop-chlr-22973-1'.
      |Teradata Parallel Transporter Load Operator Version 14.10.00.12
      |tpt_writer: private log not specified
      |tpt_reader: DataConnector Producer operator Instances: 1
      |tpt_reader: ECI operator ID: 'tpt_reader-22973'
      |tpt_reader: Operator instance 1 processing file '/tmp/artemisia/c1239888-a5f9-4f29-90c5-3da237e4859a/input.pipe'.
      |tpt_writer: connecting sessions
      |tpt_writer: preparing target table
      |tpt_writer: entering Acquisition Phase
      |tpt_writer: entering Application Phase
      |tpt_writer: Statistics for Target Table:  'sandbox.chlr_test2'
      |tpt_writer: Total Rows Sent To RDBMS:      1000010
      |tpt_writer: Total Rows Applied:            1000000
      |tpt_writer: Total Rows in Error Table 1:   3
      |tpt_writer: Total Rows in Error Table 2:   2
      |tpt_writer: Total Duplicate Rows:          1
      |tpt_reader: Total files processed: 1.
      |tpt_reader: TPT19229 50 error rows sent to error file /tmp/artemisia/c1239888-a5f9-4f29-90c5-3da237e4859a/Artemisia/error.txt
      |tpt_writer: disconnecting sessions
      |tpt_writer: Total processor time used = '2.03 Second(s)'
      |tpt_writer: Start : Wed Sep 14 19:13:04 2016
      |tpt_writer: End   : Wed Sep 14 19:14:46 2016
      |Job step LOAD_TABLE completed successfully
      |Job sandbox.chlr_test2 completed successfully
      |Job start: Wed Sep 14 19:12:52 2016
      |Job end:   Wed Sep 14 19:14:46 2016
      |
    """.stripMargin

  "TPTLoadLogParser" must "parse the TPT log" in {
    val parser = new TPTLoadLogParser(new NullOutputStream)
    parser.write(sampleTPTLog.getBytes())
    parser.rowsSent must be (1000010)
    parser.rowsLoaded must be (1000000)
    parser.rowsErr1 must be (3)
    parser.rowsErr2 must be (2)
    parser.rowsDuplicate must be (1)
    parser.errorFileRows must be (50)
  }

}
