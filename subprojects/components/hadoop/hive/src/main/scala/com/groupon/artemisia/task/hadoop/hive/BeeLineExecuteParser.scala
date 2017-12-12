package com.groupon.artemisia.task.hadoop.hive

import java.io.OutputStream

import scala.collection.mutable
import com.groupon.artemisia.inventory.io.OutputLogParser
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}


/**
  * Created by chlr on 12/11/17.
  */
class BeeLineExecuteParser(stream: OutputStream) extends OutputLogParser(stream) {

  private val regex = """.*(Partition|Table)[\s]+([^\{]+)(\{.*\})?[\s]+stats:[\s]+\[numFiles=\d+, numRows=(\d+), .*\]""".r

  private val repository = mutable.Map[String, Long]()

  /**
    * parse each line and perform any side-effecting operation necessary
    *
    * @param line line to be parsed.
    */
  override def parse(line: String): Unit = line match {
    case regex(_, tableName, _, rows) =>  repository.update(tableName, rows.toLong + repository.getOrElse(tableName, 0L))
    case _ => ()
  }


  def getData: Config = repository.toMap.foldLeft(ConfigFactory.empty)({
    case (acc, (key, value)) => acc.withValue(key, ConfigValueFactory.fromAnyRef(value))
  })

}
