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
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.util.DocStringProcessor.StringUtil
import com.groupon.artemisia.util.HoconConfigUtil

/**
  * Created by chlr on 6/7/16.
  */
trait TaskLike {

  /**
    * name of the task
    */
  def taskName: String

  /**
    *
    */
  def defaultConfig: Config

  /**
    * one line info about the task
    */
  val info: String

  /**
    * task info in brief
    */
  val desc: String


  /**
    * task output config definition
    */
  val outputConfig: Option[Config]


  /**
    * task output description
    */
  val outputConfigDesc: String


  final def outputConfigContent = {
    outputConfig match {
      case Some(x) =>
          s"""
             |     ${HoconConfigUtil.render(x.root()).ident(5)}
             |
             | $outputConfigDesc
           """.stripMargin

      case None => s"""${outputConfigDesc.ident(1)}"""
    }
  }


  /**
    * Sequence of config keys and their associated values
    */
  def paramConfigDoc: Config


  /**
    *
    * @param component name of the component
    * @return config structure of the task
    */
  final def configStructure(component: String): String = {
   val config = ConfigFactory parseString  s"""
       | {
       |   ${Keywords.Task.COMPONENT} = $component
       |   ${Keywords.Task.TASK} = $taskName
       | }
     """.stripMargin
    HoconConfigUtil.render(config.withValue(Keywords.Task.PARAMS, paramConfigDoc.root()).root())
  }


  /**
    * definition of the fields in task param config
    */
  def fieldDefinition: Map[String, AnyRef]


  /**
    * returns the brief documentation of the task
    *
    * @param component name of the component
    * @return task documentation
    */
  def doc(component: String) = {
    s"""
       |### $taskName:
       |
       |
       |#### Description:
       |
       | $desc
       |
       |#### Configuration Structure:
       |
       |
       |      ${configStructure(component).ident(5)}
       |
       |
       |#### Field Description:
       |
       | ${TaskLike.displayFieldListing(fieldDefinition) ident 1}
       |
       |#### Task Output:
       |
       |${outputConfigContent.ident(0)}
       |
     """.stripMargin

  }

  /**
    * config based constructor for task
    *
    * @param name a name for the task
    * @param config param config node
    */
  def apply(name: String, config: Config): Task


}

object TaskLike {

  def displayFieldListing(fieldDefinition: Map[String, AnyRef], ident: Int = 0): String  = {
    fieldDefinition map {
      case (field, value: String) => s"${" " * ident}* $field: $value"
      case (field, value: (String, Seq[String]) @unchecked) if value._2.head.isInstanceOf[String]=>
        s"""${" " * ident}* $field: ${value._1}
           |${value._2 map {x => s"${" " * (ident + 4)}* $x"} mkString "\n" }""".stripMargin
      case (field, value: (String, Seq[(String, String)]) @unchecked) if value._2.head.isInstanceOf[(String, String)] =>
        s"""${" " * ident}* $field: ${value._1}
           |${value._2 map {x => s"${" " * (ident + 4)}* ${x._1}: ${x._2}"} mkString "\n" }""".stripMargin
      case (field, value: Map[String, AnyRef] @unchecked) => s"${" " * ident}* $field:\n${displayFieldListing(value, ident+3)}"
    } mkString System.lineSeparator()
  }

}
