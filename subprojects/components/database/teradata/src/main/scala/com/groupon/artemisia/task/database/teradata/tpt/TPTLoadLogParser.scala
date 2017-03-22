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

import java.io.OutputStream
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import com.groupon.artemisia.inventory.io.OutputLogParser

/**
  * Created by chlr on 9/11/16.
  */

/**
  * This class parses the stdout of the TPT job and captures important job parameters from the log.
  * The job parameters includes.
  * $ - tpt job name
  * $ - rows sent
  * $ - rows applied to table
  * $ - rows in err1 table
  * $ - rows in err2 table
  * $ - duplicate rows
  * $ - rows in error file
  *
  * @param stream stream where data has to be relayed once it is parsed.
  */
class TPTLoadLogParser(stream: OutputStream)
  extends OutputLogParser(stream)
  with BaseTPTLogParser {

  private val jobIdRgx = ".*Job id is (\\b[\\w]*.?[\\w]+-[\\d]+\\b),.*".r
  private val jobLogFileRgx = s"Job log:[\\s]+(.+)".r
  private val rowsSentRgx = "tpt_writer: Total Rows Sent To RDBMS:[\\s]+(\\d+)".r
  private val rowsAppliedRgx = "tpt_writer: Total Rows Applied:[\\s]+(\\d+)".r
  private val rowsErr1Rgx = "tpt_writer: Total Rows in Error Table 1:[\\s]+(\\d+)".r
  private val rowsErr2Rgx = "tpt_writer: Total Rows in Error Table 2:[\\s]+(\\d+)".r
  private val rowsDuplicateRgx = "tpt_writer: Total Duplicate Rows:[\\s]+(\\d+)".r
  private val errorFileRowsRgx = "tpt_reader:.+\\b(\\d+)\\b.*error rows sent to error file.*".r

  override var jobId: String  = _
  override var jobLogFile: String = _
  var rowsSent: Long = 0
  var rowsLoaded: Long = 0
  var rowsErr1: Long = 0
  var rowsErr2: Long = 0
  var rowsDuplicate: Long = 0
  var errorFileRows: Long = 0


  override def parse(line: String): Unit = {
    line match {
      case jobIdRgx(x) => jobId = x
      case rowsSentRgx(x) => rowsSent = x.toLong
      case rowsAppliedRgx(x) => rowsLoaded = x.toLong
      case rowsErr1Rgx(x) => rowsErr1 = x.toLong
      case rowsErr2Rgx(x) => rowsErr2 = x.toLong
      case rowsDuplicateRgx(x) => rowsDuplicate = x.toLong
      case errorFileRowsRgx(x) => errorFileRows = x.toLong
      case jobLogFileRgx(x) => jobLogFile = x
      case _ => ()
    }
  }

  override def toConfig = {
    ConfigFactory.empty()
      .withValue("sent", ConfigValueFactory.fromAnyRef(rowsSent))
      .withValue("loaded", ConfigValueFactory.fromAnyRef(rowsLoaded))
      .withValue("err_table1", ConfigValueFactory.fromAnyRef(rowsErr1))
      .withValue("err_table2", ConfigValueFactory.fromAnyRef(rowsErr2))
      .withValue("duplicate", ConfigValueFactory.fromAnyRef(rowsDuplicate))
      .withValue("err_file", ConfigValueFactory.fromAnyRef(errorFileRows))
      .withValue("source", ConfigValueFactory.fromAnyRef(rowsSent + errorFileRows))
      .withValue("rejected", ConfigValueFactory.fromAnyRef(rowsErr1 + rowsErr2 + rowsDuplicate + errorFileRows))
  }

}
