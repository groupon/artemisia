package com.groupon.artemisia.task.hadoop.hive

import java.io.OutputStream
import com.groupon.artemisia.inventory.io.OutputLogParser
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

/**
  * Created by chlr on 12/11/17.
  */
class BeeLineReadParser(stream: OutputStream) extends OutputLogParser(stream) {

  var counter: Int = 0
  val buffer = new StringBuffer()
  var header: String = _
  var row: String = _

  /**
    * parse each line and perform any side-effecting operation necessary
    *
    * @param line line to be parsed.
    */
  override def parse(line: String): Unit = {
    if (counter <= 4 && line.length > 0) {
      counter += 1
      buffer.append(line+System.lineSeparator)
    }
  }

  /**
    * get parsed data
    * @return
    */
  def getData: Config = {
    val parsedData = buffer.toString
      .split(System.lineSeparator()).zipWithIndex
      .collect({ case (x,y) if y % 2 == 1 => x })
      .map(x => x.split('|').map(_.trim).filterNot(_ == ""))
    parsedData match {
      case Array(cols, fields) => cols.zip(fields).foldLeft(ConfigFactory.empty)({
        case (acc, (key, value)) => acc.withValue(key, ConfigValueFactory.fromAnyRef(value))
      })
    }
  }

}