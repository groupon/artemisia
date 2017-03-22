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
import com.groupon.artemisia.task.database.DBInterface
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import scala.util.{Failure, Success, Try}


/**
  *
  */
class TPTStreamLogParser(stream: OutputStream) extends OutputLogParser(stream)
  with BaseTPTLogParser {

  private val errorFileRowsRgx = "tpt_reader:\\s\\b[\\w]+\\b\\s(\\d+)\\serror rows sent to error file.+".r
  private val appliedRowsRgx = "tpt_writer: Rows Inserted:\\s+(\\d+)".r
  private val jobIdRgx = ".*Job id is (\\b[\\w]*.?[\\w]+-[\\d]+\\b),.*".r
  private val jobLogFileRgx = s"Job log:[\\s]+(.+)".r

  var appliedRows: Long = 0
  var rejectedRows: Long = 0
  var errorTableRows: Long = 0
  var errorFileRows: Long = 0
  override var jobId: String = _
  override var jobLogFile: String = _


  /**
    * parse each line and perform any side-effecting operation necessary
    *
    * @param line line to be parsed.
    */
  override def parse(line: String): Unit = {
    line match {
      case errorFileRowsRgx(rows) => errorFileRows = rows.toLong
      case appliedRowsRgx(rows) => appliedRows = rows.toLong
      case jobIdRgx(x) => jobId = x
      case jobLogFileRgx(x) => jobLogFile = x
      case _ => ()
    }
  }

  override def toConfig = {
    ConfigFactory.empty()
      .withValue("loaded", ConfigValueFactory.fromAnyRef(appliedRows))
      .withValue("error-file", ConfigValueFactory.fromAnyRef(errorFileRows))
      .withValue("error-table", ConfigValueFactory.fromAnyRef(errorTableRows))
      .withValue("rejected", ConfigValueFactory.fromAnyRef(errorTableRows + errorFileRows))
      .withValue("source", ConfigValueFactory.fromAnyRef(appliedRows + errorTableRows + errorFileRows))
  }


  def updateErrorTableCount(tableName: String)(implicit dBInterface: DBInterface) = {
    Try(dBInterface.queryOne(s"SELECT count(*) as cnt FROM ${tableName}_ET", printSQL = false).as[Long]("cnt")) match {
      case Success(cnt) => errorTableRows = cnt
      case Failure(th) => ()
    }
  }


}
