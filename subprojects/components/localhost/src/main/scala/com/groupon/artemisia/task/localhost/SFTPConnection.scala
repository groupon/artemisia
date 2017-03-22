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

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.task.settings.ConnectionHelper
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 6/22/16.
  */

case class SFTPConnection(host: String, port: Int = 22, username: String, password: Option[String] = None, pkey: Option[File] = None) {

}

object SFTPConnection extends ConnectionHelper {

  type T = SFTPConnection

  val defaultConfig = ConfigFactory parseString
    s"""
       | {
       |   port = 22
       | }
     """.stripMargin

  val configStructure =
    s"""|  "connection_[0]" = sftp_connection_name
        |  "connection_[1]" = {
        |     hostname = "sftp-host-name @required"
        |     port = "sftp-host-port @default(22)"
        |     username = "sftp-username @required"
        |     password = "sftppassword @optional(not required if key based authentication is used)"
        |     pkey = "/home/user/.ssh/id_rsa @optional(not required if username/password authentication is used)"
        |   }""".stripMargin


  val fieldDefinition = Map(
    "hostname" -> "hostname of the sftp-server",
    "port" -> "sftp port number",
    "username" -> "username to be used for sftp connection",
    "password" -> "optional password for sftp connection if exists",
    "pkey" -> "optional private key to be used for the connection"
  )

  def apply(config: Config): SFTPConnection = {
    SFTPConnection(
      host = config.as[String]("hostname"),
      port = config.getAs[Int]("port").getOrElse(22),
      username = config.as[String]("username"),
      password = config.getAs[String]("password"),
      pkey = config.getAs[String]("pkey") map { new File(_) }
    )
  }



}

