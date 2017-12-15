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

package com.groupon.artemisia.task.localhost

import java.nio.file.Paths
import com.groupon.artemisia.core.{AppLogger, Keywords}
import com.groupon.artemisia.task.localhost.util.ProcessRunner
import com.groupon.artemisia.task.{Task, TaskLike}
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.groupon.artemisia.util.Util
import com.typesafe.config.{Config, ConfigFactory}

/**
 * Created by chlr on 2/21/16.
 */
class ScriptTask(name: String = Util.getUUID, script: String,interpreter: String = "/bin/sh" ,cwd: String = Paths.get("").toAbsolutePath.toString
                 , env: Map[String,String] = Map()
                 , parseOutput: Boolean = false) extends Task(name: String) {


  val processRunner : ProcessRunner = new ProcessRunner(interpreter)
  val scriptFileName = "script.sh"

  override def setup(): Unit = {
    this.writeToFile(script,scriptFileName)
  }

  override def work(): Config = {
    AppLogger info s"executing script"
    AppLogger info Util.prettyPrintAsciiBanner(script, heading = "script")
    var result: (String, String, Int) = null
    writeToFile(script, scriptFileName)
    result = processRunner.executeFile(cwd, env)(getFileHandle(scriptFileName).toString)

    AppLogger debug s"stdout detected: ${result._1}"
    AppLogger debug s"stderr detected: ${result._2}"

    assert(result._3 == 0, "Non Zero return code detected")
    ConfigFactory parseString { if (parseOutput) result._1 else "" }
  }

  override def teardown(): Unit = {}

}

object ScriptTask extends TaskLike {

  override val taskName = "ScriptTask"

  override val info = "executes script with customizable interpreter"

  override val defaultConfig: Config = ConfigFactory parseString
    s"""
      | {
      |   interpreter = "/bin/sh"
      |   parse-output = no
      |   env = {}
      | }
    """.stripMargin

  override def apply(name: String, config: Config) = {
    new ScriptTask (
      name
     ,script = config.as[String]("script")
     ,interpreter = config.as[String]("interpreter")
     ,cwd = config.getAs[String]("cwd").getOrElse(Paths.get("").toAbsolutePath.toString)
     ,env = config.asMap[String]("env")
     ,parseOutput = config.as[Boolean]("parse-output")
    )
  }

  override val desc: String =
    s"""
      |$taskName is used to execute scripts in a shell native to the operating system;
      |For example *Bash* in *Linux*. The content of the `script` node is flushed into a temporary script file
      |and the script file is executed by the shell interpreter specified in `interpreter` node.
    """.stripMargin

  override val paramConfigDoc = {
    ConfigFactory parseString
    s"""
       |{
       |  ${Keywords.Task.PARAMS} = {
       |     script = "echo Hello World @required"
       |     interpreter = "/usr/local/bin/sh @default(/bin/sh)"
       |     cwd = "/var/tmp @default(<your current working directory>)"
       |     env = "{ foo = bar, hello = world } @default(<empty object>)"
       |     parse-output = "yes @default(false)"
       |   }
       |}
     """.stripMargin
  }

  override val fieldDefinition = Map(
    "script" -> "string whose content while be flushed to a temp file and executed with the interpreter",
    "interpreter" -> "the interpreter used to execute the script. it can be bash, python, perl etc",
    "cwd" -> "set the current working directory for the script execution",
    "env" -> "environmental variables to be used",
    "parse-output" -> "parse the stdout of script which has to be a Hocon config (Json superset) and merge the result to the job config"
  )

  override val outputConfig: Option[Config] = {
    val config = ConfigFactory parseString
      s"""
         |{
         |  foo = bar
         |}
       """.stripMargin
    Some(config)
  }

  override val outputConfigDesc: String =
    """
      | This task has two modes depending on parse-output flag being set to true or not.
      |
      |  * when it is set to true the output of the script is parsed as Hocon config. In the above example we emit a
      |  object with key foo and value bar.
      |
      |  * when it is set to false the task emits an empty config object.
    """.stripMargin

}
