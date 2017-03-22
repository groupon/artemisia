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

package com.groupon.artemisia.task.database

import java.sql.{Connection, DriverManager}
import com.groupon.artemisia.task.settings.DBConnection

/**
 * Created by chlr on 4/27/16.
 */
object TestDBInterFactory {
  
  
  def withDefaultDataLoader(table: String, database: String = "test",mode: Option[String] = None, createTestTable: Boolean = true) = {
    val dbInterface: DBInterface = new DBInterface with DefaultDBBatchImporter with DefaultDBExporter  {
      override def getNewConnection: Connection = {
        val modeOption = (mode map { x => s"MODE=$x;" }).getOrElse("")
        Class.forName("org.h2.Driver")
        DriverManager.getConnection(s"jdbc:h2:mem:$database;${modeOption}DB_CLOSE_DELAY=-1","","")
      }
    }
    processDbInterface(dbInterface, table)
    dbInterface
  }
  
  
  private def processDbInterface(dbInterface: DBInterface, table: String) = {
    dbInterface.execute(
      s"""CREATE TABLE IF NOT EXISTS $table
         |(
         | col1 int,
         | col2 varchar(10),
         | col3 boolean,
         | col4 tinyint,
         | col5 bigint,
         | col6 decimal(6,2),
         | col7 time,
         | col8 date,
         | col9 timestamp,
         |)
         |""".stripMargin)
    dbInterface.execute(s"DELETE FROM $table")
    dbInterface.execute(s"INSERT INTO $table VALUES (1, 'foo', true, 100, 10000000, 87.3, '12:30:00', '1945-05-09', '1945-05-09 12:30:00')")
    dbInterface.execute(s"INSERT INTO $table VALUES (2, 'bar', false, 100, 10000000, 8723.38, '12:30:00', '1945-05-09', '1945-05-09 12:30:00')")
  }

  /**
   * This is stubbed ConnectionProfile object primarily to be used along with H2 database which doesn't require a connectionProfile object to work with.
   * @return ConnectionProfile object
   */
  val stubbedConnectionProfile = DBConnection("","","","",-1)

}


