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

package com.groupon.artemisia.task.database

import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.database.DBUtil.ResultSetIterator
import com.groupon.artemisia.task.settings.DBConnection

/**
 * Created by chlr on 4/27/16.
 */
class DBUtilSpec extends TestSpec {


  "DBUtil" must "parse table name with databasename" in {
    val tableName = "database.tablename"
    DBUtil.parseTableName(tableName) match {
      case (Some(db),table) => {
        db must be ("database")
        table must be ("tablename")
      }
      case _ => ???
    }
  }

  it must "iterator over an resultset as expected with data" in {
    val dBInterface = TestDBInterFactory.withDefaultDataLoader("db_utils")
    val rs = dBInterface.query("select * from db_utils")
   val data = new ResultSetIterator[(Int,String,Boolean,Short,Long,Float)](rs) {
      override def generateRow: (Int, String, Boolean, Short, Long, Float) = {
        (rs.getInt(1), rs.getString(2), rs.getBoolean(3), rs.getShort(4), rs.getLong(5), rs.getFloat(6))
      }
    }.toSeq
    data must contain theSameElementsInOrderAs Seq(
      (1,"foo",true,100: Short,10000000L,87.3F)
      ,(2,"bar",false,100: Short,10000000L,8723.38F)
    )
  }

  it must "iterator over an empty resultset and return an empty seq" in {

    val dBInterface = TestDBInterFactory.withDefaultDataLoader("db_utils")
    dBInterface.execute("DELETE FROM db_utils")
    val rs = dBInterface.query("select * from db_utils")
    val data = new ResultSetIterator[(Int,String,Boolean,Short,Long,Float)](rs) {
      override def generateRow: (Int, String, Boolean, Short, Long, Float) = {
        (rs.getInt(1), rs.getString(2), rs.getBoolean(3), rs.getShort(4), rs.getLong(5), rs.getFloat(6))
      }
    }.toSeq
    data mustBe empty
  }


  it must "generate connection url" in {

    DBUtil.dbConnectionUrl("mysql", DBConnection("servername", "alkenhayn", "verdun", "france", 1000)) match {
      case Some((x,y)) =>
        x must be ("com.mysql.jdbc.Driver")
        y must be ("jdbc:mysql://servername:1000/france?zeroDateTimeBehavior=convertToNull")
      case None => fail("mysql db failed to generate connection url")
    }

    DBUtil.dbConnectionUrl("teradata", DBConnection("servername", "alkenhayn", "verdun1", "france", 1000)) match {
      case Some((x, y)) =>
        x must be ("com.teradata.jdbc.TeraDriver")
        y must be ("jdbc:teradata://servername:1000/france")
      case None => fail("teradata db failed to generate connection url")
    }

    DBUtil.dbConnectionUrl("postgres", DBConnection("servername", "alkenhayn", "verdun1", "france", 1000)) match {
      case Some((x, y)) =>
        x must be ("org.postgresql.Driver")
        y must be ("jdbc:postgresql://servername:1000/france")
      case None => fail("postgres db failed to generate connection url")
    }

    DBUtil.dbConnectionUrl("unknown_db", DBConnection("servername", "alkenhayn", "verdun1", "france", 1000)) must be (None)

  }



}
