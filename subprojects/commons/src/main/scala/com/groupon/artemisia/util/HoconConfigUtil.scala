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

import com.typesafe.config._
import org.apache.commons.lang3.StringEscapeUtils
import com.groupon.artemisia.inventory.exceptions.{InvalidSettingException, SettingNotFoundException}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

/**
 * Created by chlr on 3/18/16.
 */
object HoconConfigUtil {

  implicit val anyRefReader = new ConfigReader[AnyRef] {
    override def read(config: Config, path: String): AnyRef = {
      config.getAnyRef(path)
    }
  }

  implicit val anyRefListReader = new ConfigReader[List[AnyRef]] {
    override def read(config: Config, path: String): List[AnyRef] = {
      ( config.getAnyRefList(path).asScala map { _.asInstanceOf[AnyRef] } ).toList
    }
  }

  implicit val booleanReader = new ConfigReader[Boolean] {
    override def read(config: Config, path: String): Boolean = {
      config.getBoolean(path)
    }
  }

  implicit val booleanListReader = new ConfigReader[List[Boolean]] {
    override def read(config: Config, path: String): List[Boolean] = {
      config.getBooleanList(path).asScala.toList map { _.booleanValue() }
    }
  }

  implicit val byteReader = new ConfigReader[Byte] {
    override def read(config: Config, path: String): Byte = {
      config.getInt(path).asInstanceOf[Byte]
    }
  }

  implicit val configReader = new ConfigReader[Config] {
    override def read(config: Config, path: String): Config = {
      config.getConfig(path)
    }
  }

  implicit val configValueReader = new ConfigReader[ConfigValue] {
    override def read(config: Config, path: String): ConfigValue = {
      config.getValue(path)
    }
  }

  implicit val configListReader = new ConfigReader[List[Config]] {
    override def read(config: Config, path: String): List[Config] = {
      config.getConfigList(path).asScala.toList
    }
  }

  implicit val doubleReader = new ConfigReader[Double] {
    override def read(config: Config, path: String): Double = {
      config.getDouble(path)
    }
  }

  implicit val doubleListReader = new ConfigReader[List[Double]] {
    override def read(config: Config, path: String): List[Double] = {
      config.getDoubleList(path).asScala.toList map {_.toDouble }
    }
  }

  implicit val durationReader = new ConfigReader[FiniteDuration] {
    override def read(config: Config, path: String): FiniteDuration = {
      DurationParser(config.getString(path)).toFiniteDuration
    }
  }


  implicit val durationListReader = new ConfigReader[List[FiniteDuration]] {
    override def read(config: Config, path: String): List[FiniteDuration] = {
     config.getStringList(path).asScala.map(x => DurationParser(x).toFiniteDuration).toList
    }
  }

  implicit val intReader = new ConfigReader[Int] {
    override def read(config: Config, path: String): Int = {
      config.getInt(path)
    }
  }

  implicit val intListReader = new ConfigReader[List[Int]] {
    override def read(config: Config, path: String): List[Int] = {
      config.getIntList(path).asScala.toList map { _.toInt }
    }
  }

  implicit val longReader = new ConfigReader[Long] {
    override def read(config: Config, path: String): Long = {
      config.getLong(path)
    }
  }

  implicit val longListReader = new ConfigReader[List[Long]] {
    override def read(config: Config, path: String): List[Long] = {
      config.getLongList(path).asScala.toList map { _.toLong }
    }
  }

  implicit val memoryReader = new ConfigReader[MemorySize] {
    override def read(config: Config, path: String): MemorySize = {
      new MemorySize(config.getString(path))
    }
  }

  implicit val memoryListReader = new ConfigReader[List[MemorySize]] {
    override def read(config: Config, path: String): List[MemorySize] = {
      config.getStringList(path).asScala.map(new MemorySize(_)).toList
    }
  }


  implicit val charReader = new ConfigReader[Char] {
    override def read(config: Config, path: String): Char = {
      val data = config.getString(path)
      val parsedData = data.length match {
        case 1 => data
        case _ => StringEscapeUtils.unescapeJava(data)
      }
      require(parsedData.length == 1, "Character length is not 1")
      parsedData.toCharArray.apply(0)
    }
  }

  implicit val stringReader = new ConfigReader[String] {
    override def read(config: Config, path: String): String = {
      val str = config.getString(path)
      HoconConfigEnhancer.stripLeadingWhitespaces(str)
    }
  }

  implicit val stringListReader = new ConfigReader[List[String]] {
    override def read(config: Config, path: String): List[String] = {
      config.getStringList(path).asScala.toList
    }
  }

  /**
   *
   * @param config implicit function that converts Config to ConfigResolver object
   * @return ConfigResolver object
   */
  implicit def configToConfigEnhancer(config: Config): HoconConfigEnhancer = {
    new HoconConfigEnhancer(config)
  }

  def render(configValue: ConfigValue, indent: Int = 0): String = {
    configValue match {
      case config: ConfigObject => {
        val result =  s"""|{
                          |${config.keySet().asScala.toList.sorted map { x => s"   $x = ${render(config.toConfig.getValue(s""""$x""""), indent+1)}" } mkString s"${System.lineSeparator()}" }
                          |}""".stripMargin
        result.split(System.lineSeparator()) map { x => s"${" "*indent}$x" } mkString System.lineSeparator()
      }
      case config: ConfigList => {
        s"""|[${config.unwrapped().asScala map { ConfigValueFactory.fromAnyRef } map { render(_, indent+1) } mkString ", "}]""".stripMargin
      }
      case config => {
        config.render(ConfigRenderOptions.concise())
      }
    }
  }

  trait ConfigReader[T] {
    def read(config: Config, path: String): T
  }

  implicit class Handler(val config: Config) {

    def asMap[T: ConfigReader](key: String): Map[String,T] = {
      val configObject = config.getConfig(key).root()
      val result = configObject.keySet().asScala map { x => x -> configObject.toConfig.as[T](x) }
      result.toMap
    }

    def getAs[T: ConfigReader](key: String): Option[T] = {
      if (config.hasPath(key)) Some(as[T](key)) else None
    }

    def as[T: ConfigReader](key: String): T = {
      implicitly[ConfigReader[T]].read(config, key)
    }

    def asInlineOrFile(key: String): String = {
      if (config.hasPath(key)) {
        // dont make it config.as[String] and if you do
        // module dependency of project artemisia with all the components
        // break for some reason... this needs investigation
        HoconConfigEnhancer.stripLeadingWhitespaces(config.getString(key))
      }
      else if (config.hasPath(s"$key-file"))
        HoconConfigEnhancer.readFileContent(new File(config.getString(s"$key-file")))
      else
        throw new SettingNotFoundException(s"key $key/$key-file was not found")
    }


    /**
      * this outputs a Seq of String. the value of this key can either be a
      * string, array or a file with a optional separator character. the optional
      * separator character if present will be used to split the content of the file
      * to generate to Seq[String] to generate the result
      *
      * @param key
      * @param separator
      * @return
      */
    def asInlineArrayOrFile(key: String, separator: Char): Seq[String] = {
      if (config.hasPath(key)) {
        // dont make it config.as[String] and if you do
        // module dependency of project artemisia with all the components
        // break for some reason... this needs investigation
        config.getValue("key").valueType() match {
          case ConfigValueType.STRING => Seq(HoconConfigEnhancer.stripLeadingWhitespaces(config.getString(key)))
          case ConfigValueType.LIST => config.getStringList(key).asScala
            .map(HoconConfigEnhancer.stripLeadingWhitespaces)
          case _ => throw new InvalidSettingException(s"the key $key must either be a string or a list of string")
        }
      }
      else if (config.hasPath(s"$key-file")) {
        HoconConfigEnhancer.readFileContent(new File(config.getString(s"$key-file"))).split(separator).toSeq
      }
       else throw new SettingNotFoundException(s"key $key/$key-file was not found")
    }

  }

}


