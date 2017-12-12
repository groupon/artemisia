package com.groupon.artemisia.task.hadoop.hive

import com.groupon.artemisia.inventory.exceptions.InvalidSettingException

/**
  * Created by chlr on 12/11/17.
  */

/**
  * hive interface modes
  */
sealed trait Mode

object Mode {
  def apply(mode: String): Mode = mode.toLowerCase match {
    case "hiveserver2" => HiveServer2
    case "beeline" => Beeline
    case "cli" => HiveCLI
    case x => throw new InvalidSettingException(s"$x is not a supported value. hiveserver2, beeline, cli are the only " +
      s"supported values")
  }
}

object HiveServer2 extends Mode

object Beeline extends Mode

object HiveCLI extends Mode
