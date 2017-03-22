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

import java.io.OutputStream
import java.net.URI
import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.core.AppLogger
import com.groupon.artemisia.task.Task
import com.groupon.artemisia.task.settings.{DBConnection, ExportSetting}

/**
  * Created by chlr on 4/13/16.
  */

/**
  *
  * @param name              name of the task instance
  * @param sql               query for the export
  * @param connectionProfile Connection Profile settings
  * @param exportSetting     Export settings
  */
abstract class ExportToFile(val name: String, val sql: String, val location: URI, val connectionProfile: DBConnection, val exportSetting: ExportSetting)
  extends Task(name: String) {

  val dbInterface: DBInterface

  val target: Either[OutputStream, URI]

  val supportedModes: Seq[String]

  override protected[task] def setup(): Unit = {}

  /**
    *
    * SQL export to file
    *
    * @return Config object with key rows and values as total number of rows exports
    */
  override protected[task] def work(): Config = {
    AppLogger info s"exporting data to ${location.toString}"
    val records = dbInterface.exportSQL(sql, target, exportSetting)
    AppLogger info s"exported $records rows to ${location.toString}"
    wrapAsStats {
      ConfigFactory parseString
        s"""
           | rows = $records
           """.stripMargin
    }
  }

  override protected[task] def teardown() = {
    AppLogger debug s"closing database connection"
    dbInterface.terminate()
    target match {
      case Left(stream) => AppLogger debug s"closing OutputStream to ${location.toString}"
        stream.close()
      case Right(_) => ()
    }
  }

}





