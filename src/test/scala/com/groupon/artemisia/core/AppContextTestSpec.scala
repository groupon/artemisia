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

package com.groupon.artemisia.core

import java.io.{File, FileNotFoundException}
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.dag.Message.TaskStats
import com.groupon.artemisia.util.FileSystemUtil
import com.groupon.artemisia.util.FileSystemUtil.{FileEnhancer, withTempDirectory}
import com.groupon.artemisia.util.HoconConfigUtil.Handler


/**
*  Created by chlr on 12/4/15.
*/
class AppContextTestSpec extends TestSpec {


  override def beforeEach(): Unit = {
    //sys_var = Keywords.Config.GLOBAL_FILE_REF_VAR -> this.getClass.getResource("/global_config.conf").getFile
  }

  "The Config Object" must s"Read the Global File and merge it with default config file" in {

      val app_context = new AppContext(AppContextTestSpec.defaultTestCmdLineParams)
      app_context.init()
      app_context.payload = app_context.payload.resolve()
      info("checking if job_config is in effect")
      app_context.payload.as[String]("dummy_step1.config.table") must be ("dummy_table")
      info("checking if global_config is in effect")
      app_context.payload.as[String]("dummy_step1.config.dsn") must be ("mysql_database")
      info("checking if code config variable resolution")
      app_context.payload.as[Int]("dummy_step1.config.misc_param") must be (100)
      info("checking if reference config is available")
      app_context.payload.as[String]("foo") must be("bar")
  }

  it must "throw an FileNotFoundException when the GLOBAL File doesn't exists" in  {
      val configFile = this.getClass.getResource("/global_config.conf").getFile+"_not_exists"
      val appSetting = AppContextTestSpec.defaultTestCmdLineParams.copy(
        globalConfigFileRef = Some(configFile)
      )
      val ex = intercept[FileNotFoundException] {
         new AppContext(appSetting).init()
      }
      info("validating exception message")
      ex.getMessage must be (s"The Config file $configFile is missing")
  }

  it must "throw an FileNotFoundException when the config file doesn't exist" in  {
      val configFile = "/not_exists_file1"
      val appSetting = AppContextTestSpec.defaultTestCmdLineParams.copy(config = Some(configFile))
      info("intercepting exception")
      val ex = intercept[FileNotFoundException] {
         new AppContext(appSetting).init()
      }
      info("validating exception message")
      ex.getMessage must be(s"The Config file $configFile is missing")
  }

  it must "throw a ConfigException.Parse exception on invalid context string" in {
    val appSetting = AppContextTestSpec.defaultTestCmdLineParams.copy(context = Some("a==b==c"))
    info("intercepting exception")
    intercept[ConfigException.Parse] {
      new AppContext(appSetting).init()
    }
  }

  it must "write the checkpoint in the right file with right content" in {
    withTempDirectory("AppContextSpec") {
      workingDir => {
        val task_name = "dummy_task"
        val cmd = AppContextTestSpec.defaultTestCmdLineParams.copy(working_dir = Some(workingDir.toString))
        val appContext = new AppContext(cmd)
        appContext.init()
        appContext.commitCheckpoint(task_name, AppContextTestSpec.getTaskStatsConfigObject)
        val checkpoint = ConfigFactory.parseFile(new File(FileSystemUtil.joinPath(workingDir.toString, "checkpoint.conf")))
        info("validating end-time")
        checkpoint.getString(s"${Keywords.Checkpoint.TASK_STATES}.$task_name.${Keywords.TaskStats.END_TIME}") must be("2016-01-18 22:27:52")
        info("validating start-time")
        checkpoint.getString(s"${Keywords.Checkpoint.TASK_STATES}.$task_name.${Keywords.TaskStats.START_TIME}") must be("2016-01-18 22:27:51")
      }
    }
  }

  it must "read a checkpoint file " in {

    withTempDirectory("AppContextSpec") {
      workingDir => {
        val task_name = "dummy_task"
        val checkpointFile = new File(workingDir, "checkpoint.conf")
        checkpointFile <<=
          s"""
            |{
            |  "${Keywords.Checkpoint.PAYLOAD}": {
            |    "foo": "bar"
            |  },
            |  "${Keywords.Checkpoint.TASK_STATES}": {
            |    $task_name = {
            |      ${Keywords.TaskStats.ATTEMPT} = 1,
            |      ${Keywords.TaskStats.END_TIME} = "2016-05-23 23:11:07",
            |      ${Keywords.TaskStats.START_TIME} = "2016-05-23 23:10:56",
            |      ${Keywords.TaskStats.STATUS} = SUCCEEDED,
            |      ${Keywords.TaskStats.TASK_OUTPUT}: {
            |        "foo": "bar"
            |      }
            |    }
            |  }
            |}
          """.stripMargin
        val cmd = AppContextTestSpec.defaultTestCmdLineParams.copy(working_dir = Some(workingDir.toString))
        val appContext = new AppContext(cmd)
        appContext.init()
        val task_stats = appContext.checkpoints.taskStatRepo(task_name)
        info("validating end_time")
        task_stats.endTime must be("2016-05-23 23:11:07")
        info("validating start_time")
        task_stats.startTime must be("2016-05-23 23:10:56")
      }
    }
  }

  it must "make working_dir is configurable from cmdline" in {
    val workingDir = "/var/tmp"
    val cmdLineParam = AppContextTestSpec.defaultTestCmdLineParams.copy(working_dir = Some(workingDir))
    val appContext = new AppContext(cmdLineParam)
    appContext.init()
    appContext.workingDir must be (workingDir)
  }

  it must "make working_dir is configurable via settings node in payload" in {
    val workingDir = "/var/tmp/artemisia"
    val runID = "qwertyuiop"
    FileSystemUtil.withTempFile(fileName = "appcontext_working_dir_test") {
      file => {
        file <<=
         s"""
            |${Keywords.Config.SETTINGS_SECTION}.core.working_dir = $workingDir
            |
            |step1 = {
            | Component = SomeDummyComponent
            |}
          """.stripMargin
        val appSetting = AppSetting(cmd=Some("run"), value = Some(file.toString), run_id = Some(runID))
        info(appSetting.value.get)
        val appContext = new AppContext(appSetting)
        appContext.init()
        appContext.workingDir must be (FileSystemUtil.joinPath(workingDir,runID))
      }
    }
  }


}



object AppContextTestSpec {

  def getTaskStatsConfigObject = {
    val task_stat_config: Config = ConfigFactory parseString s"""
        |{
        |    ${Keywords.TaskStats.ATTEMPT} = 1,
        |    ${Keywords.TaskStats.END_TIME} = "2016-01-18 22:27:52",
        |    ${Keywords.TaskStats.START_TIME} = "2016-01-18 22:27:51",
        |    ${Keywords.TaskStats.STATUS} = "SUCCEEDED",
        |    ${Keywords.TaskStats.TASK_OUTPUT} = {"new_variable": 1000 }
        |}
      """.stripMargin

    TaskStats(task_stat_config)
  }


  def defaultTestCmdLineParams = {

    val job_config = Some(this.getClass.getResource("/job_config.conf").getFile)
    val code = Some(this.getClass.getResource("/code/code_with_simple_mysql_component.conf").getFile)
    val context = Some("ignore_failure=yes")
    val working_dir = None
    val globalConfigFileRef = Some(this.getClass.getResource("/global_config.conf").getFile)
    val cmd_line_params = AppSetting(cmd=Some("run"), value=code, context = context, config = job_config,
      working_dir = working_dir, globalConfigFileRef = globalConfigFileRef)
    cmd_line_params

  }


}
