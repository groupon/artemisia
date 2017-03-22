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

import org.apache.commons.mail.{Email, EmailAttachment, MultiPartEmail}
import com.groupon.artemisia.core.AppLogger
import com.groupon.artemisia.task.localhost.{EmailConnection, EmailRequest}

/**
 * Created by chlr on 6/16/16.
 */

class EmailBuilder(val emailConnection: Option[EmailConnection] = None) {

  val email = new MultiPartEmail()

  def build(emailRequest: EmailRequest): Email = {
    configureConnection(emailRequest)
    configureRequest(emailRequest)
    email
  }

  private[localhost] def configureConnection(emailRequest: EmailRequest) = {
    emailConnection match {
      case Some(connection) => {
        email.setHostName(connection.host)
        email.setSmtpPort(connection.port)
        email.setSSL(connection.ssl)
        email.setTLS(connection.tls)
        for (username <- connection.username; password <- connection.password) {
          email.setAuthentication(username, password)
        }
        connection.from foreach { x => email.setFrom(x) }
        connection.replyTo foreach { x => email.addReplyTo(x) }
      }
      case None => {
        AppLogger warn s"Email connection not set. raising Exception"
        throw new RuntimeException("Email connection not set")
      }
    }
  }

  private[localhost] def configureRequest(emailRequest: EmailRequest) = {

    emailRequest.to foreach { email.addTo }
    emailRequest.cc foreach { email.addCc }
    emailRequest.bcc foreach { email.addBcc }

    emailRequest.attachments foreach {
      case (x,y) =>
        val attachment = new EmailAttachment()
        attachment.setPath(y.toPath.toString)
        x foreach { a => attachment.setName(a) }
        email.attach(attachment)
    }

    email.setSubject(emailRequest.subject)
    email.setMsg(emailRequest.message)

  }


}


