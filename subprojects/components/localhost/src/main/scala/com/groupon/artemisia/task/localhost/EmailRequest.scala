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
import com.typesafe.config.Config
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import scala.collection.JavaConverters._

/**
 * Created by chlr on 6/15/16.
 */

/**
 *
 * @param to to address list
 * @param cc cc address list
 * @param bcc bcc address list
 * @param attachments list of attachment files
 * @param subject subject
 * @param message message
 */
case class  EmailRequest(to: Seq[String], cc: Seq[String] = Nil, bcc: Seq[String] =  Nil,
                        attachments: Seq[(Option[String], File)] = Nil, subject: String, message: String)


object EmailRequest {

  val structure =
    s"""|{
        | "to_[0]" = "xyz@example.com"
        | "to_[1]" = [ "xyz1@example.com", "xyz2@example.com" ]
        | "cc_[0]" = "xyz@example.com @optional"
        | "cc_[1]" = "[ xyz1@example.com, xyz2@example.com ] @optional"
        | "bcc_[0]" = "xyz@example.com @optional"
        | "bcc_[1]" = "[ xyz1@example.com, xyz2@example.com ] @optional"
        | "attachment_[0]" = "['/var/tmp/file1.txt', '/var/tmp/file2.txt'] @optional"
        | "attachment_[1]" = "[{'attachment1.txt', '/var/tmp/file1.txt'}, {'attachment2.txt', '/var/tmp/file2.txt'}] @optional"
        | subject = "subject"
        | message = "message"
        |}""".stripMargin


  val fieldDefinition = Map(
    "to" -> "to address list. it can either be a single email address string or an array of email address",
    "cc" -> "cc address list. same as to address both string and array is supported",
    "bcc" -> "bcc address list. same as to address both string and array is supported",
    "attachment" ->
      """
        |can be a array of strings or objects. If string it must be a path to the file. if Object it must have one key
        |and value. The key will be name of the attached file and value will be the path of the file. In the  example
        |`[{'attachment1.txt', '/var/tmp/file1.txt'}, {'attachment2.txt', '/var/tmp/file2.txt'}]`. There are two files in the attachment.
        |attachment 1 with name `attachment1.txt` is a file loaded from the path /var/tmp/file1.txt.
        |And similarly attachment 2 is a file with name `attachment2.txt` loaded from the path /var/tmp/file2.txt.
        |""".stripMargin
  )


  def apply(config: Config): EmailRequest = {

    def parseAddressConfig(field: String) = {
      config.getValue(field).unwrapped() match {
        case x: String => Seq(x)
        case x: java.util.List[String] @unchecked => x.asScala
      }
    }

    EmailRequest(
      to = parseAddressConfig("to"),
      cc = parseAddressConfig("cc"),
      bcc = parseAddressConfig("bcc"),
      attachments = if (config.hasPath("attachments")) config.as[List[AnyRef]]("attachments") map {
        case x: String => None ->new File(x)
        case y: java.util.Map[String,String] @unchecked =>  y.asScala.toSeq.head match { case (a: String, b: String) => Some(a) -> new File(b) }
      } else Nil,
      subject = config.as[String]("subject"),
      message = config.as[String]("message")
    )

  }



}
