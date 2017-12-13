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

package com.groupon.artemisia.task.hadoop.hive

import com.groupon.artemisia.util.TestUtils._
import java.io.File
import org.apache.hadoop.io.IOUtils.NullOutputStream
import com.groupon.artemisia.TestSpec

/**
  * Created by chlr on 8/7/16.
  */
class HiveCliInterfaceSpec extends TestSpec {

  "HiveCLIInterface" must "execute query and parse result for hive execute classic" in {
    runOnPosix {
      val task = new HQLExecute("hql_execute", Seq("select * from table"), HiveCLI, None) {
        val file = new File(this.getClass.getResource("/executables/hive_execute_classic").getFile)
        file.setExecutable(true)
        override protected lazy val hiveCli = new HiveCLIInterface(file.toString, new NullOutputStream, new NullOutputStream)
      }
      val result = task.execute()
      result.getInt("hql_execute.__stats__.rows-effected.test_table") must be (52)
    }
  }

  it must "execute query and parse result for hive read classic" in {
    runOnPosix {
      val task = new HQLRead("hql_read", "select * from table", HiveCLI, None) {
        val file = new File(this.getClass.getResource("/executables/hive_read").getFile)
        file.setExecutable(true)
        override protected lazy val hiveCli = new HiveCLIInterface(file.toString, new NullOutputStream, new NullOutputStream)
      }
      val result = task.execute()
      result.getInt("col1") must be (10)
      result.getString("col2") must be ("xyz")
    }
  }

  "HiveCLIInterface" must "execute query and parse result for hive execute yarn" in {
    runOnPosix {
      val task = new HQLExecute("hql_execute", Seq("select * from table"), HiveCLI, None) {
        val file = new File(this.getClass.getResource("/executables/hive_execute_yarn").getFile)
        file.setExecutable(true)
        override protected lazy val hiveCli = new HiveCLIInterface(file.toString, new NullOutputStream, new NullOutputStream)
      }
      val result = task.execute()
      result.getInt("""hql_execute.__stats__.rows-effected."chlr_db.test_table"""") must be (1097)
    }
  }

}
