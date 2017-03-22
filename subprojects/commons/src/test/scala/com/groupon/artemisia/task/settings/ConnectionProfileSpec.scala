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

import com.typesafe.config.ConfigFactory
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.core.Keywords.Connection

/**
 * Created by chlr on 4/16/16.
 */
class ConnectionProfileSpec extends TestSpec {

  "ConnectionProfile" must "correctly construct the object from config" in {
    val config = ConfigFactory parseString
     s"""
        |	{
        |		${Connection.HOSTNAME} = "database-host"
        |		${Keywords.Connection.USERNAME} = "tango"
        |		${Keywords.Connection.PASSWORD} = "bravo"
        |		${Keywords.Connection.DATABASE} = "november"
        |		${Keywords.Connection.PORT} = 1000
        |	}
      """.stripMargin

    val connectionProfile = DBConnection(config)
    connectionProfile.hostname must be ("database-host")
    connectionProfile.username must be ("tango")
    connectionProfile.password must be ("bravo")
    connectionProfile.default_database must be ("november")
    connectionProfile.port must be (1000)
  }
}
