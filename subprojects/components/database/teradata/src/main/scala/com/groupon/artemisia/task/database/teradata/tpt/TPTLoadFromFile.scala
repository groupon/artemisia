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

import java.net.URI
import com.typesafe.config.Config
import com.groupon.artemisia.task.Task
import com.groupon.artemisia.task.database.teradata.TeraUtils
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.BashUtil
import com.groupon.artemisia.util.CommandUtil._
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.groupon.artemisia.core.AppLogger._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try


/**
  * TPTLoadFromFile lets you load file to Teradata using TPT.
  * It supports Load Operator and Stream operator depending on the mode.
  * This is abstract class. you can instantiate it using the apply method
  * as factory. This is because the TPTLoadSetting has to be preprocessed
  * when the mode is set to '''auto'''.
  *
  * @param taskName name of the task
  * @param tableName target table name
  * @param location location of the file(s) to load.
  * @param connectionProfile database connection profile
  * @param loadSetting load settings
  */
abstract class TPTLoadFromFile(override val taskName: String
                     ,override val tableName: String
                     ,override val location: URI
                     ,override val connectionProfile: DBConnection
                     ,override val loadSetting: TPTLoadSetting)
  extends TPTLoad(taskName, tableName, location, connectionProfile, loadSetting) {

    /**
      *  tpt script generator
      */
    override val scriptGenerator = BaseTPTLoadScriptGen.create(tptLoadConfig, loadSetting, connectionProfile)

    /**
     * get reader Future. this Future will launch a thread
      *
      * @return
     */
    override lazy val readerFuture = {
      val textCmd = s"cat $location > $dataPipe"
      Future {
        val ret = executeShellCommand(textCmd)
        assert(ret == 0, s"command $textCmd failed with return code of $ret")
      }
    }

    override def setup() = {
      // The error table has to be truncated because when the source file(s) is empty
      // the entire TPT launch is skipped. but if there was an error table from a
      // previous run with data entries in it say for example 3 records. then the output config would
      // erroneously consider the source had 3 records and all were sent to the error table.
      // so cleaning up of the error table is necessary.
      debug("truncating error table")
      Try(dbInterface.execute(s"DELETE FROM ${tableName}_ET;", printSQL = false))
      super.setup()
    }

}

object TPTLoadFromFile extends TPTTaskHelper {

  override val taskName: String = "TPTLoadFromFile"

  override def apply(name: String, config: Config, reference: Config): Task = {
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val destinationTable = config.as[String]("destination-table")
    val loadSettings = TPTLoadSetting(config.as[Config]("load"))
    val location = new URI(config.as[String]("location"))
    TPTLoadFromFile(name, destinationTable, location ,connectionProfile, loadSettings)
  }

  /**
    * create an Instance of TPTLoadFromFile.
    * The TPTLoadSetting is transformed and optimized if auto mode is set.
    *
    * @param taskName
    * @param tableName
    * @param location
    * @param connectionProfile
    * @param loadSetting
    */
  def apply(taskName: String, tableName: String, location: URI, connectionProfile: DBConnection,
            loadSetting: TPTLoadSetting) = {
    val loadSize = BashUtil.pathSize(location.toString)
    val optimizedLoadSetting = TeraUtils.autoTuneLoadSettings(loadSize ,loadSetting)
    new TPTLoadFromFile(taskName, tableName, location, connectionProfile, optimizedLoadSetting) {
      override protected val loadDataSize: Long = loadSize
    }
  }

  override val info: String = "Load data from Local File-System to Teradata using TPT"

  override val desc: String =
    """| Load data from a local file system to Teradata. This task is supported only in POSIX OS like Linux/Mac OS X.
       |  This task also expects the TPT binary installed in the local machine. It supports two mode of operations.
       |
       |  * **default**: This uses TPT Stream operator to load data.
       |  * **fastload**: This uses TPT load operator to load data.
       |
       |  To use either of the modes set **load.mode** property to *default*, *fastload* or *auto*.
       |  when the mode is set to *auto*, one of the two modes of *default* or *fastload* is automatically selected
       |  depending on the size of the data to be loaded. The property **load.bulk-threshold** defines the threshold
       |  for selecting the *default* and *fastload* mode. for eg if **load.bulk-threshold** is defined as 50M
       |  (50 Megabytes) any file(s) whose total size is lesser than 50M will be loaded by *default* mode and any file(s)
       |  larger than this threshold will be loaded via the *fastload* mode.
       |
       |  The truncate option internally tries to delete the target table but if the target table has a fastload lock
       |  on the table the target table is dropped and re-created.
       |
    """.stripMargin

  override val outputConfig =  TPTLoad.outputConfig

  override val outputConfigDesc = TPTLoad.outputConfigDesc

}
