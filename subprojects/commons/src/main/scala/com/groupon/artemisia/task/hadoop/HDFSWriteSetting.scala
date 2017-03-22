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
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.groupon.artemisia.util.MemorySize

/**
  *
  * @param location hdfs path to write the data
  * @param replication replication factor for the file being writtten
  * @param blockSize HDFS block size of the target file
  * @param codec compression codec to use if any
  */
case class HDFSWriteSetting(location: URI, overwrite: Boolean = false, replication: Byte = 3, blockSize: Long = 67108864,
                            codec: Option[String] = None) {

  require(replication <= 5, "assert parameter cannot be greater than 5")
  codec foreach {
    x  => require(HDFSWriteSetting.allowedCodecs contains x.toLowerCase, s"$x is not a supported compression format")
  }

}

object HDFSWriteSetting extends ConfigurationNode[HDFSWriteSetting] {

  val allowedCodecs = "gzip" :: "bzip2" :: "default" :: Nil


  override val defaultConfig: Config = ConfigFactory parseString
    s"""
       | {
       |   replication = 3
       |   block-size = 64M
       |   overwrite = no
       | }
     """.stripMargin

  override val structure: Config = ConfigFactory parseString
    s"""
       | {
       |   location = "/user/hadoop/test"
       |   block-size = 120M
       |   overwrite = no
       |   codec = gzip
       |   replication = "2 @default(3) @info(allowed values 1 to 5)"
       | }
     """.stripMargin


  override val fieldDescription: Map[String, Any] = Map(
    "replication" -> "replication factor for the file. only values 1 to 5 are allowed",
    "block-size" -> "HDFS block size of the file",
    "overwrite" -> "overwrite target file it already exists",
    "codec" -> ("compression format to use. The allowed codecs are" -> allowedCodecs),
    "location" -> "target HDFS path"
  )


  def apply(config: Config): HDFSWriteSetting = {
    new HDFSWriteSetting(
      location = new URI(config.as[String]("location")),
      overwrite = config.as[Boolean]("overwrite"),
      replication = config.as[Byte]("replication"),
      blockSize = config.as[MemorySize]("block-size").toBytes,
      codec = config.getAs[String]("codec")
    )
  }

}

