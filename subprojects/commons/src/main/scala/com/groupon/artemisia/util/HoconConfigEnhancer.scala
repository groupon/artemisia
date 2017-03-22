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

package com.groupon.artemisia.util

import java.io.File
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.groupon.artemisia.task.TaskContext
import scala.collection.JavaConverters._
import scala.util.matching.Regex.Match

/**
 * Created by chlr on 4/25/16.
 */

/**
 * A pimp my Libary class for Hocon Config object that resolves quoted strings by doing DFS on the config tree
  *
  * @param root resolved Config object
 */
class HoconConfigEnhancer(val root: Config)  {

  private var reference: Config = _

  def hardResolve(reference: Config = root) = {
    this.reference = reference
    resolveConfig(root.resolveWith(reference))
  }

  private def resolveConfig(config: Config): Config = {

    val processed = for (key <- config.root().keySet().asScala) yield {
      /*
          the keys are double quoted to handle Quoted ConfigString with dot as part of the key value.
           for eg. { "foo.bar" = baz } must not be parsed as { foo = { bar = baz} } since foo.bar is a single
           quoted key
       */
      config.getAnyRef(s""""$key"""")  match {
        case x: String => key -> ConfigValueFactory.fromAnyRef(HoconConfigEnhancer.resolveString(x, this.reference))
        case x: java.lang.Iterable[AnyRef] @unchecked => key -> ConfigValueFactory.fromAnyRef(resolveList(x.asScala))
        case x: java.util.Map[String, AnyRef] @unchecked => key -> ConfigValueFactory.fromMap {
          resolveConfig(config.getConfig(s""""$key"""")).root().unwrapped()
        }
        case x => key -> ConfigValueFactory.fromAnyRef(x)
      }
    }
    processed.foldLeft(ConfigFactory.empty())({ (configValue, elems)  => configValue.withValue(s""""${elems._1}"""", elems._2) } )
  }


  private def resolveList(list: Iterable[Any]): java.lang.Iterable[Any] = {
    val processed: Iterable[Any] = for (node <- list) yield {
      node match {
        case x: java.util.Map[String,AnyRef] @unchecked => {
          resolveConfig(ConfigValueFactory.fromMap(x).toConfig).root().unwrapped()
        }
        case x: java.lang.Iterable[AnyRef] @unchecked => resolveList(x.asScala)
        case x: String => HoconConfigEnhancer.resolveString(x, this.reference)
        case x => x
      }
    }
    processed.asJava
  }

}


object HoconConfigEnhancer {

  private def resolveString(str: String, reference: Config) = {
    def replace(x: Match): String = {
      val variable = x.group(2)
      if (x.group(1) == "?")
        if (reference.hasPath(variable)) reference.getString(variable) else ""
      else
        reference.getString(variable)
    }
    val rgx = """\$\{(\??)(.+?)\}""".r
    rgx.replaceAllIn(str, replace _)
  }

  def stripLeadingWhitespaces(str: String): String = {
    val minWhiteSpace = str.split("\n") filter {
      _.trim.length > 0
    } map { """^[\s]+""".r.findFirstIn(_).getOrElse("").length} match {
      case Array() => 0
      case x => x.min
    }
    val result = str.split("\n") filter { _.trim.length > 0 } map { ("""^[\s]{"""+minWhiteSpace+"""}""").r.replaceFirstIn(_,"") }
    result.mkString("\n")
  }

  def readFileContent(file: File, reference: Config = TaskContext.payload) = {
    val content = scala.io.Source.fromFile(file).mkString
    resolveString(content,reference)
  }

}
