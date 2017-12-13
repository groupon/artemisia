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

package com.groupon.artemisia.task.hadoop.hive

import java.io.InputStream
import java.net.URI
import java.sql.{Connection, DriverManager}

import com.groupon.artemisia.core.AppLogger.info
import com.groupon.artemisia.task.database.{DBImporter, DBInterface, DefaultDBExporter}
import com.groupon.artemisia.task.settings.{DBConnection, LoadSetting}
import com.groupon.artemisia.util.Util

/**
  * Created by chlr on 8/1/16.
  */

class HiveServerDBInterface(connectionProfile: DBConnection) extends DBInterface with DBImporter with DefaultDBExporter {

  override def getNewConnection: Connection = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    DriverManager.getConnection(
      HiveServerDBInterface.makeUrl(connectionProfile),
      connectionProfile.username,
      connectionProfile.password
    )
  }

  /**
    *
    * @param sql DML query to be executed
    * @return number of records updated/deleted/inserted
    */
  override def execute(sql: String, printSQL: Boolean = true): Long = {
    if (printSQL) {
      info("executing query")
      info(Util.prettyPrintAsciiBanner(sql, "query"))
    }
    val stmt = connection.createStatement()
    val recordCnt = stmt.executeUpdate(sql)
    stmt.close()
    recordCnt
  }

  override def load(tableName: String, inputStream: InputStream, loadSetting: LoadSetting) = ???

  override def load(tableName: String, location: URI, loadSetting: LoadSetting): (Long, Long) = ???

}

object HiveServerDBInterface {

  /**
    * create jdbc url
    * @param connectionProfile
    * @return
    */
  def makeUrl(connectionProfile: DBConnection) =
    s"jdbc:hive2://${connectionProfile.hostname}:${connectionProfile.port}/${connectionProfile.default_database}"

}
