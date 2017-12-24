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

package com.groupon.artemisia.task

import java.io.{File, PrintWriter}

import com.google.common.io.Files
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.util.FileSystemUtil

import scala.io.Source


/**
 * Created by chlr on 3/3/16.
 */


/**
 * a generic class that defined lifecycle methods of a task.
 *
 * This abstract class defines lifecycle methods like setup, work, teardown.
 * Additionally it provides common helper methods like writeToFile, getFileHandle and readFile.
 *
 * @constructor creates a new instance of Task.
 *
 * @param taskName name of the task
 */

abstract class Task(val taskName: String) {

  /**
   *
   * @param content the content to write in the file.
   * @param fileName the name of the file to write the content
   * @return the java.io.File handle
   */
  protected def writeToFile(content: String, fileName: String): File = {
    val file = this.getFileHandle(fileName)
    Files.createParentDirs(file)
    val writer = new PrintWriter(file)
    writer.write(content)
    writer.close()
    file
  }

  /**
   * get File object for `fileName`
   * creates the file if the file doesn't exists
   * returns the java.io.File object for a give file `fileName` located within the working directory of the task.
   *
   * @param fileName name of the file to accessed
   * @return an instance of java.io.File representing the requested file.
   */
  protected def getFileHandle(fileName: String): File = {
    val file = new File(FileSystemUtil.joinPath(TaskContext.workingDir.toString,taskName),fileName)
    Files.createParentDirs(file)
    Files.touch(file)
    file
  }

  /**
   *
   * @param fileName name of the file to be read
   * @return String content of the file.
   */
  protected def readFile(fileName: String): String = {
    Source.fromFile(getFileHandle(fileName)).getLines().mkString
  }

  /**
   * override this to implement the setup phase
   *
   * This method should have setup phase of the task, which may include actions like
   *  - creating database connections
   *  - generating relevant files
   */
  def setup(): Unit

  /**
   * override this to implement the work phase
   *
   * this is where the actual work of the task is done, such as
   *  - executing query
   *  - launching subprocess
   *
   * @return any output of the work phase be encoded as a HOCON Config object.
   */
  def work(): Config


  /**
   * override this to implement the teardown phase
   *
   * this is where you deallocate any resource you have acquired in setup phase.
   */
  def teardown(): Unit


  /**
    *
    * @return
    */
  def execute(): Config = {
    this.setup()
    val result = this.work()
    this.teardown()
    result
  }

  /**
   * take a task stat config object and wrap up it with proper format
   * for eg: a task with node name of xyz with stats config object
   * of { foo = bar } will be transformed as
   * {{{
   *  xyx {
   *    __stats__ = {
   *        foo = bar
   *     }
   *  }
   * }}}
   *
   * @param config
   * @return
   */
  def wrapAsStats(config: ConfigValue): Config = {
    ConfigFactory.empty.withValue(s""""$taskName"""",
      ConfigFactory.empty.withValue(Keywords.TaskStats.STATS, config).root)
  }

}

