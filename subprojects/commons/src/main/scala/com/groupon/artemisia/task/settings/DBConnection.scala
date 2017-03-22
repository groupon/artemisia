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

package com.groupon.artemisia.task.settings

import com.typesafe.config._
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 4/13/16.
 */

sealed abstract class Connection

case class DBConnection(hostname: String, username: String, password: String, default_database: String, port: Int)
    extends Connection

object DBConnection extends ConnectionHelper {

  type T = DBConnection

  def structure(defaultPort: Int) =
  ConfigFactory parseString
 s""" |{
      |  ${Keywords.Connection.HOSTNAME} = "db-host @required"
      |  ${Keywords.Connection.USERNAME} = "username @required"
      |  ${Keywords.Connection.PASSWORD} = "password @required"
      |  ${Keywords.Connection.DATABASE} = "db @required"
      |  ${Keywords.Connection.PORT} = "$defaultPort @default($defaultPort)"
      | }
  """.stripMargin

  def apply(config: Config): DBConnection = {
       DBConnection(
      hostname = config.as[String](Keywords.Connection.HOSTNAME),
      username = config.as[String](Keywords.Connection.USERNAME),
      password = config.as[String](Keywords.Connection.PASSWORD),
      default_database = config.as[String](Keywords.Connection.DATABASE),
      port = config.as[Int](Keywords.Connection.PORT)
    )
  }

  /**
    * A dummy stand in DBConnection object that has all its properties/fields set to null.
    * @return
    */
  def getDummyConnection = {
    DBConnection(null, null, null, null, -1)
  }
}