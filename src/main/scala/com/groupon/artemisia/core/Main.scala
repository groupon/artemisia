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

import scopt.OptionParser
import com.groupon.artemisia.util.Util

object Main {

  var show_usage_on_error = true

  def main(args: Array[String]): Unit = {
    Thread.currentThread().setName(Keywords.APP)
    parseCmdLineArguments(args,show_usage_on_error) match {
      case cmdLineParams @ AppSetting(Some("run"), Some(_), _, _, _, _, _, _, _, _) => Command.run(cmdLineParams)
      case cmdLineParams @ AppSetting(Some("doc"), _, _, _, _, _, _, _, component, task) => Command.doc(cmdLineParams)
      case cmdLineParams @ _ => throw new IllegalArgumentException("--help to see supported options")
      }
    }

  private[core] def parseCmdLineArguments(args: Array[String],usageOnError: Boolean = true): AppSetting = {

    val parser = new OptionParser[AppSetting](Keywords.APP) {
      head(Keywords.APP)
        arg[String]("cmd") action { (x, c) =>  c.copy(cmd = Some(x)) } required() children {
        opt[String]('l', "location")  action { (x, c) => c.copy(value = Some(x)) } text "location of the job conf"
        opt[String]('w',"workdir") action { (x,c) => c.copy( working_dir = Some(x) ) } text "set the working directory for the current job"
        opt[Unit]('n',"no-checkpoint") action { (x,c) => c.copy(skip_checkpoints = true) } text "set this property skip checkpoints"
        opt[String]('r', "run-id") action { (x, c) => c.copy(run_id = Some(x)) } text "run_id for execution"
        opt[String]('c', "component") action { (x,c) => c.copy(component = Some(x)) }
        opt[String]('t', "task") action { (x, c) => c.copy(task = Some(x)) }
      } text "command options"
      opt[String]("context") valueName "k1=v1,k2=v2..." action { (x, c) => c.copy(context = Some(x)) }
      opt[String]("config") action { (x, c) => c.copy(config = Some(x)) } text "configuration file"
      override def showUsageOnError: Boolean = usageOnError
      override def errorOnUnknownArgument = true
    }

    parser.parse(args, AppSetting()).getOrElse(AppSetting())
  }

  private[core] def preProcessAppSetting(appSetting: AppSetting) = {
    appSetting.copy(globalConfigFileRef = Util.getGlobalConfigFile(scala.util.Properties.envOrNone(Keywords.Config.GLOBAL_FILE_REF_VAR)))
  }

}

