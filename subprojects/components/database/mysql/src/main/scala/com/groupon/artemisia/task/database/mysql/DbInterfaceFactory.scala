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

package com.groupon.artemisia.task.database.mysql

import java.sql.{Connection, DriverManager}
import com.groupon.artemisia.task.database._
import com.groupon.artemisia.task.settings.DBConnection


/**
 * Created by chlr on 4/13/16.
 */

/**
 * Factory object for constructing Dbinterface object
 */
object DbInterfaceFactory {

  /**
   *
   * @param connectionProfile ConnectionProfile object
   * @param mode mode can be either `default` or `native` to choose loader method
   * @return DbInterface
   */
  def getInstance(connectionProfile: DBConnection, mode: String = "default") = {
    mode match {
      case "default" => new DefaultDBInterface(connectionProfile)
      case "bulk" => new NativeDBInterface(connectionProfile)
      case _ => throw new IllegalArgumentException(s"mode '$mode' is not supported")
    }
  }

  /**
   * MySQL DBInterface with default Loader
   *
   * @param connectionProfile ConnectionProfile object
   */
  class DefaultDBInterface(connectionProfile: DBConnection) extends DBInterface with DefaultDBExporter with DefaultDBBatchImporter {
    override def getNewConnection: Connection = {
      getConnection(connectionProfile)
    }
  }

  /**
   * MySQL DBInterface with native Loader
   *
   * @param connectionProfile ConnectionProfile object
   */
  class NativeDBInterface(connectionProfile: DBConnection) extends DBInterface with MySQLDataTransporter {
    override def getNewConnection: Connection = {
      getConnection(connectionProfile)
    }
  }

  private def getConnection(connectionProfile: DBConnection) = {
    DriverManager.getConnection(s"jdbc:mysql://${connectionProfile.hostname}:${connectionProfile.port}/${connectionProfile.default_database}?" +
      s"user=${connectionProfile.username}&password=${connectionProfile.password}")
  }
  
}



