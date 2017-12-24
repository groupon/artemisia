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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.mail.MultiPartEmail
import com.groupon.artemisia.core.AppLogger
import com.groupon.artemisia.task.localhost.util.EmailBuilder
import com.groupon.artemisia.task.{Task, TaskLike}
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.groupon.artemisia.util.DocStringProcessor._

/**
 * Created by chlr on 6/15/16.
 */
class EmailTask(name: String, val emailRequest: EmailRequest, val emailConnection: Option[EmailConnection]) extends Task(name) {

  val email = new MultiPartEmail()

  override def setup(): Unit = {}

  override def work(): Config = {
    val builder = new EmailBuilder(emailConnection)
    val email = builder.build(emailRequest)
    AppLogger info s"""sending email to ${emailRequest.to mkString ","}"""
    email.send()
    ConfigFactory.empty()
  }

  override def teardown(): Unit = {}

}


object EmailTask extends TaskLike {

  override val taskName: String = "EmailTask"

  override val defaultConfig: Config = ConfigFactory.empty()
    .withValue("connection", EmailConnection.defaultConfig.root())

  override val info: String = s"$taskName is used to send Email notifications."

  override val desc: String = s"$taskName is used to send Email notifications."

  override val paramConfigDoc = {
    ConfigFactory parseString
    s"""
       |params = {
       |	  "connection_[0]" = email_connection
       |    "connection_[1]" = ${EmailConnection.structure.ident(18)}
       |	  email = ${EmailRequest.structure.ident(18)}
       |}
     """.stripMargin
  }

  override val fieldDefinition = Map(
    "connection" -> EmailConnection.fieldDefinition,
    "email" -> EmailRequest.fieldDefinition
  )

  override def apply(name: String, config: Config): Task = {
    val emailConnection = if (config.hasPath("connection"))
      Some(EmailConnection.parseConnectionProfile(config.getValue("connection")))
    else
      None
    val emailRequest = EmailRequest(config.as[Config]("email"))
    new EmailTask(name, emailRequest, emailConnection)
  }

  override val outputConfig: Option[Config] = None

  override val outputConfigDesc = "This task outputs a empty config object"

}
