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

package com.groupon.artemisia.task.localhost.util

import com.typesafe.config._
import com.groupon.artemisia.task.ConfigurationNode
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 12/4/16.
 */

case class RestEndPoint(method: String = "get",
                        url: String,
                        header: Seq[(String, String)] = Nil,
                        body: Option[ConfigValue] = None,
                        payloadType: String = "json") {

}

object RestEndPoint extends ConfigurationNode[RestEndPoint] {


  val allowedHttpMethods = Seq("get", "head", "put", "post", "patch", "delete")

  val allowedPayloadTypes = Seq("json", "xml", "text")


  def apply(config: Config) = {
    RestEndPoint(
      config.as[String]("method"),
      config.as[String]("url"),
      config.asMap[String]("headers").toSeq,
      config.getAs[ConfigValue]("payload"),
      config.as[String]("payload-type")
    )
  }

  override val defaultConfig: Config = ConfigFactory.empty()
  .withValue("method", ConfigValueFactory.fromAnyRef("get"))
  .withValue("headers", ConfigFactory.empty().root())
  .withValue("payload-type", ConfigValueFactory.fromAnyRef("json"))

  override val structure: Config = ConfigFactory parseString
    """
      |{
      |  method = "post @default(get)"
      |  url = "http://localhost:9000/rest/api"
      |  headers = {
      |     X-AUTH-KEY = "b84b90f0-405b-4c16-a3ae-b89e4c253ec8"
      |     foo =  bar
      |  }
      |  body = {
      |    "message" = "Hello World"
      |  }
      |  payload-type = json
      |}
    """.stripMargin

  override val fieldDescription: Map[String, Any] = Map(
    "method" -> ("HTTP method call. allowed values are" -> allowedHttpMethods),
    "url" -> "url to call",
    "headers" -> "dictionary of key and values to be applied as header",
    "body" -> "request body of the rest call",
    "payload-type" -> ("type of payload to use. allowed types are" -> allowedPayloadTypes)
  )

}
