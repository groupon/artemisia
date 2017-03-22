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

import java.io._
import java.nio.file.{Files, Paths}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import com.groupon.artemisia.core.{AppLogger, Keywords}

/**
 * Created by chlr on 11/29/15.
 */


/**
 * an object with utility functions.
 *
 */
object Util {


  /**
    * get effective global config file
    * @param globalConfigFileRef config file set as environment variable
    * @return effective config file
    */
  def getGlobalConfigFile(globalConfigFileRef: Option[String], defaultConfig: String = Keywords.Config.USER_DEFAULT_CONFIG_FILE) = {
    globalConfigFileRef match {
      case Some(configFile) =>  Some(configFile)
      case None if Files exists Paths.get(defaultConfig) => Some(defaultConfig)
      case None => None
    }
  }

  /**
   *
   * @param path path of the HOCON file to be read
   * @return parsed Config object of the file
   */
  def readConfigFile(path: File): Config = {
    if(!path.exists()) {
      AppLogger error s"requested config file $path not found"
      throw new FileNotFoundException(s"The Config file $path is missing")
    }
    ConfigFactory parseFile path
  }


  /**
    * convert character to unicode code point
    *
    * {{{
    *
    * }}}
    * @param char input character
    * @return
    */
  def unicodeCode(char: Char) = {
    "\\u" + Integer.toHexString(char | 0x10000).substring(1)
  }


  /**
   * generates a UUID
   * scala> unicodeCode('a')
   * res0: String = \u0061
   * @return UUID
   */
  def getUUID = {
    java.util.UUID.randomUUID.toString
  }

  /**
   * prints stacktrace of an Exception
   *
   * @param ex Throwable object to be print
   * @return string of the stacktrace
   */
  def printStackTrace(ex: Throwable) = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    ex.printStackTrace(pw)
    sw.toString
  }

  /**
   **
   * @return current time in format "yyyy-MM-dd HH:mm:ss"
   */
  def currentTime : String = {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
      formatter.print(new DateTime())
  }

  def prettyPrintAsciiBanner(content: String, heading: String, width: Int = 80): String = {
   s"""
      |${"=" * (width / 2) } $heading ${"=" * (width / 2)}
      |$content
      |${"=" * (width / 2) } $heading ${"=" * (width / 2)}
    """.stripMargin
  }

  /**
    * An utlity function to generate markdown compatible ascii table for the two dimensional input
    *
    * {{{
    * val in = Array(Array("Country", "Captial"), Array("USA", "Washington"), Array("UK", "London"), Array("Russia", "Moscow"), Array("Japan", "Tokyo"))
    * print(prettyPrintAsciiTable(in))
    *
    * | Country  | Capital     |
    * | ---------| ------------|
    * | USA      | Washington  |
    * | UK       | London      |
    * | Russia   | Moscow      |
    * | Japan    | Tokyo       |
    * }}}
    *
    * @param content two dimensional array representation of the table
    * @return content in ascii table string
    */
  def prettyPrintAsciiTable(content: Array[Array[String]]): Seq[String] = {

    val tableDimensions = content.foldLeft(for(i <- 1 to content(0).length) yield 0 ) {
      (carry , input) => {
        carry zip input map ( x => x._1.max(x._2.length) )
      }
    } map { _ + 2 }

    def composeRow(row: Array[String], divider: Boolean = false) = { row zip tableDimensions map {
      x => s"|${if(divider)"-" else " "}${x._1}${" " * (x._2 - x._1.length)}" } mkString ""
    }

    content.toList match {
      case head :: Nil => composeRow(head) :: composeRow(tableDimensions.map("-"* _ ).toArray) :: Nil
      case head :: tail => {
        val x = composeRow(head) :: composeRow(tableDimensions.map("-" * _).toArray, divider = true) :: tail.map(composeRow(_))
        ((x mkString s"|${System.lineSeparator()}") + "|") split System.lineSeparator
      }
      case Nil => throw new RuntimeException("content cannot be empty")
    }
  }


  /**
    * convert a map (key of String and value of AnyRef) to Hocon config
    * @param map input map
    * @return Hocon config object
    */
  def mapToConfig(map: Map[String,Any]) = {
    map.foldLeft(ConfigFactory.empty()){
      (carry, input) => carry withValue (s""""${input._1}"""", ConfigValueFactory.fromAnyRef(input._2))
    }
  }



}
