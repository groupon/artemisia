package com.groupon.artemisia.task.hadoop.hive

import java.io.File

import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.HoconConfigUtil._
import com.groupon.artemisia.util.TestUtils.runOnPosix
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.hadoop.io.IOUtils.NullOutputStream

/**
  * Created by chlr on 12/24/17.
  */
class BeelineInterfaceSpec extends TestSpec {

  "BeelineInterfaceSpec" must "execute hive queries in beeline mode" in {
    runOnPosix {
      val task = new HQLExecute("hql_execute", Seq("select * from table"), Beeline,
        Some(DBConnection.getDummyConnection)) {
        val file = new File(this.getClass.getResource("/executables/beeline_execute").getFile)
        file.setExecutable(true)
        override protected lazy val beeLineCli = new BeeLineInterface(file.toString,
          DBConnection.getDummyConnection, new NullOutputStream(), new NullOutputStream())
      }
      val result = task.execute()
      result.as[Long]("hql_execute.__stats__.rows-effected.chlr_db.parquet_test_1") must be (38295615)
    }
  }

  it must "read and parse data in HQLRead" in {
    runOnPosix {
      val task = new HQLRead("hql_execute", "select * from table", Beeline, Some(DBConnection.getDummyConnection)) {
        val file = new File(this.getClass.getResource("/executables/beeline_hql_read").getFile)
        file.setExecutable(true)
        override protected lazy val beeLineCli = new BeeLineInterface(file.toString,
          DBConnection.getDummyConnection, new NullOutputStream(), new NullOutputStream())
      }
      val result = task.execute()
      result.as[Long]("cnt") must be (38295615)
    }
  }


  it must "construct HQLExecute task from config" in {
    val config = ConfigFactory.empty()
      .withValue("mode", ConfigValueFactory.fromAnyRef("beeline"))
      .withValue("sql-file", ConfigValueFactory.fromAnyRef(this.getClass.getResource("/insert.hql").getFile))
    val task = HQLExecute("hql_read", config, ConfigFactory.empty())
    task.sql match {
      case Seq(x,y) =>
        x must be ("DELETE FROM database.test_table_1")
        y must be ("INSERT INTO OVERWRITE database.test_table_1\nSELECT * FROM database.test_table_2")
    }
  }

}
