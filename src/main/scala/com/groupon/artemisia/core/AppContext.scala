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

import java.io.File
import java.nio.file.Paths

import com.groupon.artemisia.core.AppContext.{DagSetting, Logging}
import com.groupon.artemisia.core.BasicCheckpointManager.CheckpointData
import com.groupon.artemisia.dag.Message.TaskStats
import com.groupon.artemisia.inventory.exceptions.UnknownComponentException
import com.groupon.artemisia.task.{BaseComponent, Component, JavaAPIType, ScalaAPIType, TaskContext, _}
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.groupon.artemisia.util.{FileSystemUtil, Util}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

import scala.concurrent.duration.FiniteDuration


/**
 *  Created by chlr on 11/28/15.
 */


class AppContext(private val cmdLineParam: AppSetting) {


  val skipCheckpoints: Boolean = cmdLineParam.skipCheckpoints
  val globalConfigFile: Option[String] = cmdLineParam.globalConfigFileRef
  val runId: String = cmdLineParam.runId.getOrElse(Util.getUUID)

  // Always keep the payload of Appcontext and its smaller cousin TaskContext in sync.
  // this is done by leveraging the setter of payload attribute
  private var actualPayload: Config = ConfigFactory.empty()
  def payload = actualPayload
  def payload_=(config: Config): Unit = {
    TaskContext.payload = config
    actualPayload = config
  }
  actualPayload = getConfigObject

  val logging: Logging =  AppContext.parseLoggingFromPayload(payload.as[Config](s"${Keywords.Config.SETTINGS_SECTION}.logging"))
  val dagSetting: DagSetting = AppContext.parseDagSettingFromPayload(payload.as[Config](s"${Keywords.Config.SETTINGS_SECTION}.dag"))
  val workingDir: String = computeWorkingDir

  // checkpointManager can be initialized only after working dir is initialized
  // and workdir can be initialized only after initial payload instance is initialized

  protected def checkpointMgr = if (skipCheckpoints) new BasicCheckpointManager else new FileCheckPointManager(checkpointFile)

  payload = checkpointMgr.checkpoints.adhocPayload withFallback payload
  val componentMapper: Map[String, Component] = payload.asMap[String](s"${Keywords.Config.SETTINGS_SECTION}.components").map({
    case (name,component) => (name, Class.forName(component).getConstructor(classOf[String]).newInstance(name))
  }).map({
    case (name, component: Component) => name -> component
    case (name, component: JComponent) => name -> component.convert
  })


  /**
    * get component
    * @param name
    * @return
    */
  def getComponent(name: String): Component = {
    val component = payload.asMap[String](s"${Keywords.Config.SETTINGS_SECTION}.components").get(name) match {
      case Some(x) => Class.forName(x).getConstructor(classOf[String]).newInstance(name).asInstanceOf[BaseComponent]
      case None => throw new UnknownComponentException(s"component $name not found. make sure the component is " +
        s"configured in ${Keywords.Config.SETTINGS_SECTION}.${Keywords.Config.COMPONENT} section")
    }
    component.componentType match {
      case ScalaAPIType => component.asInstanceOf[Component]
      case JavaAPIType => component.asInstanceOf[JComponent].convert
    }
  }



  TaskContext.setWorkingDir(Paths.get(this.workingDir))

  /**
   * merge all config objects (Global, Code, Context) to provide unified code config object
   * @return full unified config object
   */
  private[core] def getConfigObject: Config = {
    val empty_object = ConfigFactory.empty()
    val reference = ConfigFactory parseFile new File(System.getProperty(Keywords.Config.SYSTEM_DEFAULT_CONFIG_FILE_JVM_PARAM))
    val context = (cmdLineParam.context map ( ConfigFactory parseString _ )).getOrElse(empty_object)
    val config_file = (cmdLineParam.config map { x => Util.readConfigFile(new File(x)) }).getOrElse(empty_object)
    val code = (cmdLineParam.cmd filter { _ == "run" } map
      { x => Util.readConfigFile(new File(cmdLineParam.value.get)) }).getOrElse(empty_object)
    val global_config_option = (globalConfigFile map { x => Util.readConfigFile(new File(x)) } ).getOrElse(empty_object)
    context withFallback config_file withFallback code withFallback global_config_option withFallback reference
  }

  override def toString = {
    val options = ConfigRenderOptions.defaults() setComments false setFormatted true setOriginComments false setJson true
    payload.root().render(options)
  }

  /**
   * @return checkpoint file for the session
   */
  def checkpointFile = new File(FileSystemUtil.joinPath(workingDir,Keywords.Config.CHECKPOINT_FILE))

  /**
   *
   * @param taskName
   * @param taskStats
   */
  def commitCheckpoint(taskName: String, taskStats: TaskStats) = {
    checkpointMgr.save(taskName, taskStats)
    payload = checkpointMgr.checkpoints.adhocPayload withFallback payload
  }

  /**
   * 
   * @return checkpoint data encapsulated in CheckPointData object
   */
  def checkpoints: CheckpointData = checkpointMgr.checkpoints



  /**
   * compute the effective working directory. The directory selection has the following precedence
   *  a) the one assigned via command line param
   *  b) the one defined in the settings node of the payload
   *  c) deterministic folder created in the temp directory of the system
   * @return working dir selected for the job
   */
  private[core] def computeWorkingDir = {
    val configAssigned  = payload.getAs[String]("__settings__.core.working_dir") map { FileSystemUtil.joinPath(_,runId) }
    val cmdLineAssigned = cmdLineParam.workingDir
    val defaultAssigned = FileSystemUtil.joinPath(FileSystemUtil.baseDir.toString,runId)
    cmdLineAssigned.getOrElse(configAssigned.getOrElse(defaultAssigned))
  }

}


object AppContext {

  private[core] case class DagSetting(attempts: Int, concurrency: Int, heartbeat_cycle: FiniteDuration,
                                        cooldown: FiniteDuration, disable_assertions: Boolean, ignore_conditions: Boolean)
  private[core] case class Logging(console_trace_level: String, file_trace_level: String)

  def parseLoggingFromPayload(payload: Config) = {
    Logging(console_trace_level = payload.as[String]("console_trace_level"), file_trace_level = payload.as[String]("file_trace_level"))
  }

  def parseDagSettingFromPayload(payload: Config) = {
    DagSetting(attempts = payload.as[Int]("attempts"), concurrency = payload.as[Int]("concurrency"),
      heartbeat_cycle = payload.as[FiniteDuration]("heartbeat_cycle"), cooldown = payload.as[FiniteDuration]("cooldown"),
      disable_assertions = payload.as[Boolean]("disable_assertions"), ignore_conditions = payload.as[Boolean]("ignore_conditions"))
  }

}


