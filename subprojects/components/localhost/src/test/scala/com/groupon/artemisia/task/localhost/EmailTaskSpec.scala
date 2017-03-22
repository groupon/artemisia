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
import javax.mail.internet.InternetAddress
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.localhost.util.EmailBuilder
import com.groupon.artemisia.util.FileSystemUtil.withTempFile
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._

/**
 * Created by chlr on 6/18/16.
 */
class EmailTaskSpec extends TestSpec {

  "EmailRequest" must "construct itself with config 1" in {
    val config = ConfigFactory parseString
      """
        | {
        |   to = ["x@example.com", "y@example.com"]
        |   cc = "a@example.com"
        |   bcc = "b@example.com"
        |   attachments = [/var/tmp/file1.txt, /var/tmp/file2.txt]
        |   subject = subject
        |   message = message
        | }
      """.stripMargin

    val request = EmailRequest(config)

    request.to must be (Seq("x@example.com", "y@example.com"))
    request.cc must be (Seq("a@example.com"))
    request.bcc must be (Seq("b@example.com"))
    request.attachments must be (Seq(None -> new File("/var/tmp/file1.txt"), None -> new File("/var/tmp/file2.txt")))
  }

  it must "construct itself with config 2" in {
    val config = EmailTaskSpec.defaultEmailRequestConfig

    val request = EmailRequest(config)

    request.to must be (Seq("x@example.com"))
    request.cc must be (Seq("a@example.com", "b@example.com"))
    request.bcc must be (Seq("p@example.com", "q@example.com"))
    request.attachments must be (Seq(Some("File1.txt") -> new File("/var/tmp/file1.txt"), Some("File2.txt") -> new File("/var/tmp/file2.txt")))
  }

  "EmailConnect" must "construct itself with a config" in {
    val config =  EmailTaskSpec.defaultConnectionConfig withFallback EmailConnection.defaultConfig

    val connection = EmailConnection(config)

    connection.host must be ("smtp-server")
    connection.port must be (25)
    connection.username must be (Some("scott"))
    connection.password must be (Some("tiger"))
    connection.from must be (Some("xyz@example.com"))
    connection.replyTo must be (Some("abc@example.com"))
    connection.ssl must be (false)
    connection.tls must be (false)
  }

  "EmailBuilder" must "build the email object" in {
    withTempFile(fileName = "email_builder.txt") {
      file =>
          val request = EmailRequest(EmailTaskSpec.defaultEmailRequestConfig) copy (attachments = Seq(Some("File.txt") -> file))
          val connection = EmailConnection(EmailTaskSpec.defaultConnectionConfig withFallback EmailConnection.defaultConfig)
          val builder = new EmailBuilder(Some(connection))
          val email = builder.build(request)
          email.getToAddresses.asScala map { _.asInstanceOf[InternetAddress].getAddress }  must be(Seq("x@example.com"))
          email.getCcAddresses.asScala map { _.asInstanceOf[InternetAddress].getAddress }  must be(Seq("a@example.com", "b@example.com"))
          email.getBccAddresses.asScala map { _.asInstanceOf[InternetAddress].getAddress }  must be(Seq("p@example.com", "q@example.com"))
          email.getHostName must be ("smtp-server")
          email.getSmtpPort must be ("25")
          email.getFromAddress.getAddress must be ("xyz@example.com")
          email.getReplyToAddresses.asScala map { _.asInstanceOf[InternetAddress].getAddress } must be (Seq("abc@example.com"))
          email.getSubject must be ("subject")
    }
  }

}

object EmailTaskSpec {

  val defaultConnectionConfig = ConfigFactory parseString
    """
      |  {
      |    host = "smtp-server"
      |    port = 25
      |    username = scott
      |    password = tiger
      |    from = "xyz@example.com"
      |    reply-to = "abc@example.com"
      |  }
    """.stripMargin


  val defaultEmailRequestConfig = ConfigFactory parseString
    """
      | {
      |   to = "x@example.com"
      |   cc = ["a@example.com", "b@example.com"]
      |   bcc = ["p@example.com", "q@example.com"]
      |   attachments = [{ "File1.txt" = "/var/tmp/file1.txt"}, { "File2.txt" = "/var/tmp/file2.txt" }]
      |   subject = subject
      |   message = message
      | }
    """.stripMargin

}
