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

package com.groupon.artemisia.task.hadoop

import java.net.URI
import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.task.ConfigurationNode
import com.groupon.artemisia.util.CommandUtil
import com.groupon.artemisia.util.HoconConfigUtil.Handler


/**
  *
  * @param location HDFS path
  * @param codec compression algorithm
  */
case class HDFSReadSetting(location: URI, codec: Option[String] = None, cliMode: Boolean = true,
                           cliBinary: String = "hadoop") {
  if (!cliMode) {
    codec foreach {
      x => require(HDFSWriteSetting.allowedCodecs contains x.toLowerCase, s"$x is not a supported compression format")
    }
  }

  def getCLIBinaryPath = {
    CommandUtil.getExecutablePath(this.cliBinary) match {
      case Some(binary) => binary
      case None => throw new RuntimeException(s"binary $cliBinary was not found in PATH")
    }
  }
}

object HDFSReadSetting extends ConfigurationNode[HDFSReadSetting] {

  val allowedCodecs = "gzip" :: "bzip2" :: "default" :: Nil

  override val defaultConfig: Config = ConfigFactory parseString
    """
      |{
      |  cli-mode = yes
      |  cli-binary = hadoop
      |}
    """.stripMargin

  override val structure: Config = ConfigFactory parseString
    s"""
       | {
       |   location = /var/tmp/input.txt
       |   codec = gzip
       |   cli-mode = "yes @default(yes)"
       |   cli-binary = "hdfs @default(hadoop) @info(use either hadoop or hdfs)"
       | }
     """.stripMargin

  override val fieldDescription: Map[String, Any] = Map(
    "location" -> "target HDFS path",
    "codec" -> ("compression format to use. This field is relevant only if local-cli is false. The allowed codecs are" -> allowedCodecs),
    "cli-mode" -> "boolean field to indicate if the local installed hadoop shell utility should be used to read data",
    "cli-binary" -> "hadoop binary to be used for reading. usually it's either hadoop or HDFS. this field is relevant when cli-mode field is set to yes"
  )

  override def apply(config: Config): HDFSReadSetting = {
    new HDFSReadSetting(
      location = new URI(config.as[String]("location"))
      ,codec = config.getAs[String]("codec")
      ,cliMode = config.as[Boolean]("cli-mode")
      ,cliBinary = config.as[String]("cli-binary")
    )
  }
}