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

import com.typesafe.config.{ConfigFactory, Config}
import com.groupon.artemisia.task.settings.ConnectionHelper
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 6/15/16.
 */

/**
 *
 * @param host smtp host address
 * @param port smtp port
 * @param username stmp username
 * @param password stmp password
 * @param ssl enable ssl
 * @param tls enable tls
 * @param from from address
 * @param replyTo default reply to address
 */
case class EmailConnection(host: String, port: Int, username: Option[String], password: Option[String],
                           ssl: Boolean, tls: Boolean, from: Option[String] = None, replyTo: Option[String] = None) {

}

object EmailConnection extends ConnectionHelper {

  type T = EmailConnection

  def structure =
    """|{
       |  host = "host @required"
       |  port = "-1 @required"
       |  username = "username"
       |  password = "password"
       |  ssl = "no @default(no) @type(boolean)"
       |  tls = "no @default(no) @type(boolean)"
       |  from = "xyz@example.com"
       |  reply-to ="xyx@example.com"
       |}""".stripMargin

  val fieldDefinition = Map(
    "host" -> "SMTP host address",
    "port" -> "port of the stmp server",
    "username" -> "username used for authentication",
    "password" -> "password used for authentication",
    "ssl" -> "boolean field enabling ssl",
    "tls" -> "boolean field for enabling tls",
    "from" -> "from address to be used",
    "reply-to" -> "replies to the sent email will be addressed to this address"
  )

   val defaultConfig = ConfigFactory parseString
   s"""
      |{
      |  ssl = no
      |  tls = no
      |}
    """.stripMargin

  def apply(config: Config): EmailConnection = {
    EmailConnection(
      host = config.as[String]("host"),
      port = config.as[Int]("port"),
      username = config.getAs[String]("username"),
      password = config.getAs[String]("password"),
      ssl = config.as[Boolean]("ssl"),
      tls = config.as[Boolean]("tls"),
      from = config.getAs[String]("from"),
      replyTo = config.getAs[String]("reply-to")
    )
  }


}
