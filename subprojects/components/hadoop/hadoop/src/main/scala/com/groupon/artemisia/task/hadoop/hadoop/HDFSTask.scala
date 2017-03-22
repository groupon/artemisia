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

package com.groupon.artemisia.task.hadoop.hadoop

import java.io.ByteArrayOutputStream
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.groupon.artemisia.task.{Task, TaskLike}
import com.groupon.artemisia.util.CommandUtil._


/**
  * Created by chlr on 10/11/16.
  */


/**
  *
  * @param taskName name of the task
  * @param action hdfs action to be performed
  * @param arguments list of arguments to be hdfs command
  * @param hdfsBin path to the hdfs binary
  */
class HDFSTask(override val taskName: String,
               val action: String,
               val arguments: Seq[String],
               val hdfsBin: Option[String] = None)
  extends Task(taskName) {


  protected val hdfs = hdfsBin.getOrElse(getExecutableOrFail("hdfs"))

  protected val command = hdfs :: "dfs" :: s"-$action"  :: Nil ++ arguments

  /**
    * override this to implement the setup phase
    *
    * This method should have setup phase of the task, which may include actions like
    * - creating database connections
    * - generating relevant files
    */
  override protected[task] def setup(): Unit = {}

  /**
    * override this to implement the work phase
    *
    * this is where the actual work of the task is done, such as
    * - executing query
    * - launching subprocess
    *
    * @return any output of the work phase be encoded as a HOCON Config object.
    */
  override protected[task] def work(): Config = {
    val (stdout, stderr) = new ByteArrayOutputStream() -> new ByteArrayOutputStream()
    executeCmd(command, stdout = stdout, stderr = stderr)
    wrapAsStats {
      ConfigFactory.empty()
        .withValue("stdout", ConfigValueFactory.fromAnyRef(stdout.toString))
        .withValue("stderr", ConfigValueFactory.fromAnyRef(stderr.toString))
    }
  }


  /**
    * override this to implement the teardown phase
    *
    * this is where you deallocate any resource you have acquired in setup phase.
    */
  override protected[task] def teardown(): Unit = {}

}

object HDFSTask extends TaskLike {

  override val taskName: String = "HDFSTask"

  /**
    * Sequence of config keys and their associated values
    */
  override def paramConfigDoc: Config = ConfigFactory parseString
    s"""
       | {
       |   hdfs-bin = "/usr/bin/hdfs @optional"
       |   action = "copyFromLocal @required"
       |   args = [
       |         "/user/artemisia/srcfile"
       |         "/var/path/target_file"
       |        ]
       | }
     """.stripMargin

  /**
    *
    */
  override def defaultConfig: Config = ConfigFactory.empty()

  /**
    * definition of the fields in task param config
    */
  override def fieldDefinition: Map[String, AnyRef] = Map (
    "hdfs-bin" -> "path to the hdfs/hadoop binary file" ,
    "action" -> ( "dfs action to be performed. supported actions are " ->
            Seq(
              "chmod",
              "chown",
              "copyFromLocal"
            )
        ),
    "args" -> "arguments to be passed"
  )

  /**
    * config based constructor for task
    *
    * @param name   a name for the task
    * @param config param config node
    */
  override def apply(name: String, config: Config): Task = {
    new HDFSTask(name
      ,config.as[String]("action")
      ,config.as[List[String]]("args")
      ,config.getAs[String]("hdfs-bin")
    )
  }

  override val outputConfig: Option[Config] = Some {
    ConfigFactory parseString
      s"""
         | {
         |   stdout = "stdout of the hdfs command"
         |   stderr = "stderr of the hdfs command"
         | }
       """.stripMargin
  }

  override val info: String = "This task lets you execute DFS shell commands supported by HDFS"

  override val desc: String =
    """
      |HDFSTask lets you perform DFS related operations. This tasks composes the HDFS shell commands
      |and executes it. Some of the commands that you can execute are like
      |  * chmod
      |  * copyFromFile
      |  * rmdir
    """.stripMargin

  override val outputConfigDesc =
    """
      |The resultant HDFS command's stdout and stderr is included in the task's output.
    """.stripMargin

}
