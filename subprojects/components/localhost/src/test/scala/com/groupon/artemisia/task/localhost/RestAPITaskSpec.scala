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

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.BeforeAndAfterAll
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.task.localhost.util.RestEndPoint
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 12/11/16.
  */
class RestAPITaskSpec extends TestSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    TestRestServer.start()
  }

  "RestAPITask" must "handle post json response" in {
    val restEndPoint = RestEndPoint(
      method = "post",
      url = s"${TestRestServer.address}/post/json",
      body = Some(ConfigFactory.parseString("{ foo = bar }").root),
      payloadType = "json"
    )
    val task = new RestAPITask("test", restEndPoint, emitOutput = true)
    val output = task.execute()
    output.as[String]("foo") must be ("bar")
  }

  it must "handle post xml response" in {
    val restEndPoint = RestEndPoint(
      method = "post",
      url = s"${TestRestServer.address}/post/xml",
      body = Some(ConfigValueFactory.fromAnyRef("<foo> <bar> baz </bar></foo>")),
      payloadType = "xml"
    )
    val task = new RestAPITask("test", restEndPoint, emitOutput = false)
    val output = task.execute()
    output.as[String](s"${Keywords.TaskStats.STATS}.body") must be ("<foo> <bar> baz </bar></foo>")
  }


  it must "handle get string response" in {
    val restEndPoint = RestEndPoint(method = "get", payloadType = "text",
      url = s"${TestRestServer.address}/get/string", header = Seq("__foo" -> "bar"))
    val task = new RestAPITask("test", restEndPoint)
    val output = task.execute()
    output.as[String](s"${Keywords.TaskStats.STATS}.body") must be ("Hello World")
    output.as[List[String]](s"${Keywords.TaskStats.STATS}.header.__foo") must contain only ("bar")
  }

  it must "post string and receive string with headers" in {
    val restEndPoint = RestEndPoint(method = "post", url = s"${TestRestServer.address}/post/text",
      body = Some(ConfigValueFactory.fromAnyRef("The Game is afoot")))
    val task = new RestAPITask("test", restEndPoint)
    val output = task.execute()
    output.as[String](s"${Keywords.TaskStats.STATS}.body") must be ("The Game is afoot")
  }

  it must "construct itself from config" in {
    val config = ConfigFactory parseString
      s"""
         | {
         |   request = {
         |      url = "http://api.examples.com/post/json"
         |      method = get
         |      headers = {
         |        foo = bar
         |      }
         |      payload-type = "json"
         |   }
         |   emit-output = yes
         |   allowed-status-codes = [105]
         | }
       """.stripMargin
    val task = RestAPITask("test", config).asInstanceOf[RestAPITask]
    task.restEndPoint.url must be ("http://api.examples.com/post/json")
    task.restEndPoint.header must be (Seq("foo" -> "bar"))
    task.restEndPoint.method must be ("get")
    task.restEndPoint.payloadType must be ("json")
    task.emitOutput must be (true)
    task.allowedStatusCodes must be (Seq(105))
  }


  it must "throw exception when invalid payload-type is set" in {
    val restEndPoint = RestEndPoint(method = "get",
      payloadType = "potato",
      body = Some(ConfigValueFactory.fromAnyRef("bingo")),
      url="http://www.example.com")
    val task = new RestAPITask("test", restEndPoint)
    val ex = intercept[IllegalArgumentException] {
      task.execute()
    }
    ex.getMessage mustBe "requirement failed: potato is not one of the allowed http methods. allowed payload types are " +
      "List(json, xml, text)"
  }


  it must "fail if the status-code is not one of the allowed status code" in {
    val restEndPoint = RestEndPoint(method = "get", payloadType = "text", url = s"${TestRestServer.address}/get/string")
    val task = new RestAPITask("test", restEndPoint, allowedStatusCodes = Seq(100))
    val ex = intercept[AssertionError]{
      task.execute()
    }
    ex.getMessage must be ("assertion failed: unexpected http status code. 200. allowed status codes are 100")
  }


  override def afterAll(): Unit = {
    info("calling close on the HTTP Server")
    TestRestServer.stop()
  }

}
