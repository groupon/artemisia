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

import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.inventory.exceptions.UnknownTaskException
import com.groupon.artemisia.util.Util
/**
 * Created by chlr on 3/3/16.
 */

/**
  * Component usually represents a Data system such as Database, Spark Cluster or Localhost
  * @param name name of the component
  */
abstract class Component(val name: String) {


  /**
    * list of supported task name
    */
  val tasks: Seq[TaskLike]


  /**
    * default config applicable to all task
    */
  val defaultConfig: Config


  /**
    * consolidated config structure for the component
    */
  final def consolidateDefaultConfig = tasks.foldLeft(ConfigFactory.empty.withValue(name, ConfigFactory.empty.root())) {
       (carry: Config, task: TaskLike) =>
        ConfigFactory.empty
          .withValue(s"$name.${task.taskName}",task.defaultConfig.root() withFallback defaultConfig)
          .withFallback(carry)
  }

  /**
   * returns an instance of [[Task]] configured via the config object
   *
   * {{{
   *   dispatch("ScriptTask","mySampleScriptTask",config)
   * }}}
   *
   *
   * @param task task the Component has to execute
   * @param name name assigned to the instance of the task
   * @param config HOCON config payload with configuration data for the task
   * @return an instance of [[Task]]
   */
  def dispatchTask(task: String, name: String, config: Config): Task = {
    tasks filter { _.taskName == task } match {
      case x :: Nil => x.apply(name, config
                                  .withFallback(TaskContext.getDefaults(this.name, task))
                                  .withFallback(x.defaultConfig)
                                  .withFallback(defaultConfig))

      case Nil => throw new UnknownTaskException(s"unknown task $task in component $name")
      case _ => throw new RuntimeException(s"multiple tasks named $task is register component $name")
    }
  }

  /**
    * one line description of the Component
    */
  val info: String

  /**
    * A brief overview of the components and the tasks it supports.
    */
  final def doc = {
    val taskTable: Seq[Array[String]] =  Array("Task", "Description") +: tasks.map(x => Array(x.taskName, x.info))

    s"""/
        /$name
        /${"=" * name.length}
        /
        /$info
        /
        /${Util.prettyPrintAsciiTable(taskTable.toArray).mkString(System.lineSeparator())}
        /
     """.stripMargin('/')
  }


  /**
    * get documentation of the task
    * @param task name of the task
    */
  def taskDoc(task: String): String = {
    tasks filter { _.taskName == task } match {
      case x :: Nil => x.doc(name)
      case Nil => throw new UnknownTaskException(s"unknown task $task in component $name")
      case _ => throw new RuntimeException(s"multiple tasks named $task is register component $name")
    }
  }

}
