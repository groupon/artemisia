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

package com.groupon.artemisia.task.database.teradata

import java.sql.{Connection, DriverManager}
import com.groupon.artemisia.task.database.{DefaultDBBatchImporter, DefaultDBExporter, DBInterface}
import com.groupon.artemisia.task.settings.DBConnection


/**
  * Factory object for constructing Dbinterface object
  */
object DBInterfaceFactory {

  /**
    *
    * @param connectionProfile ConnectionProfile object
    * @param mode mode can be either `default` or `native` to choose loader method
    * @return DbInterface
    */
  def getInstance(connectionProfile: DBConnection, mode: String = "default") = {
    mode match {
      case "default" => new DefaultDBInterface(connectionProfile, None)
      case "fastload" => new TeraDBInterface(connectionProfile, Some("fastload"))
      case "fastexport" => new TeraDBInterface(connectionProfile, Some("fastexport"))
      case _ => throw new IllegalArgumentException(s"mode '$mode' is not supported")
    }
  }

  class DefaultDBInterface(connectionProfile: DBConnection, mode: Option[String]) extends DBInterface
      with DefaultDBBatchImporter with DefaultDBExporter {
    override def getNewConnection: Connection = {
      getConnection(connectionProfile, mode)
    }
  }

  /**
    * Teradata DBInterface with specialized Fastload/FastExport
    *
    * @param connectionProfile ConnectionProfile object
    */
  class TeraDBInterface(connectionProfile: DBConnection, mode: Option[String]) extends DBInterface with TeraDataTransporter {
    override def getNewConnection: Connection = {
        getConnection(connectionProfile, mode)
    }
  }


  private def getConnection(connectionProfile: DBConnection, mode: Option[String]) = {
    DriverManager.getConnection(
      s"""jdbc:teradata://${connectionProfile.hostname}/${connectionProfile.default_database}," +
      s"DBS_PORT=${connectionProfile.port}${mode.map(x => s",type=$x").getOrElse("")}"""
      , connectionProfile.username
      , connectionProfile.password)
  }



}