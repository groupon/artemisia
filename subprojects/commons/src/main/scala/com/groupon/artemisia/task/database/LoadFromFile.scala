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

import java.io.InputStream
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.groupon.artemisia.core.AppLogger
import com.groupon.artemisia.task.Task
import com.groupon.artemisia.task.settings.{DBConnection, LoadSetting}

/**
 * Created by chlr on 4/30/16.
 */

/**
 * An abstract task to load data into a table
 *
 * @param name name for the task
 * @param tableName destination table to be loaded
 * @param connectionProfile connection details for the database
 * @param loadSetting load setting details
 */
abstract class LoadFromFile(val name: String, val tableName: String, val location: URI, val connectionProfile: DBConnection,
                            val loadSetting: LoadSetting) extends Task(name) {

  val dbInterface: DBInterface

  /**
    * inputStream to read data from
    */
  val source: Either[InputStream, URI]

  /**
    * list of supported modes
    */
  protected val supportedModes: Seq[String]

  override def setup() = {
    require(supportedModes contains loadSetting.mode, s"unsupported mode ${loadSetting.mode}")
    if (loadSetting.truncate) {
      AppLogger info s"truncating table $tableName"
      dbInterface.execute(s"DELETE FROM $tableName",printSQL =  false)
    }
  }

  /**
   * Actual data export is done in this phase.
   * Number of records loaded is emitted in stats node
    *
    * @return any output of the work phase be encoded as a HOCON Config object.
   */
  override def work(): Config = {
    val (totalRows, rejectedCnt) = dbInterface.loadTable(tableName, source, loadSetting)
    AppLogger info s"${totalRows - rejectedCnt} rows loaded into table $tableName"
    AppLogger info s"$rejectedCnt row were rejected"
    wrapAsStats {
      ConfigFactory.empty
        .withValue("loaded", ConfigValueFactory.fromAnyRef(totalRows-rejectedCnt))
        .withValue("rejected", ConfigValueFactory.fromAnyRef(rejectedCnt)).root()
    }
  }

   override def teardown(): Unit = {
    AppLogger debug s"closing database connection"
    dbInterface.terminate()
     source match {
       case Left(stream) => AppLogger debug s"closing InputStream from ${location.toString}"
                            stream.close()
       case Right(_) => ()
     }
   }
}



