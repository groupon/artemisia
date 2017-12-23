package com.groupon.artemisia.task

import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.util.HoconConfigUtil
import com.typesafe.config.{Config, ConfigFactory}


/**
  * Created by chlr on 12/22/17.
  */

abstract class BaseTaskLike {


  val taskType: APIType

  /**
    * name of the task
    */
  def taskName: String

  /**
    *
    */
  def defaultConfig: Config

  /**
    * one line info about the task
    */
  val info: String

  /**
    * task info in brief
    */
  val desc: String


  /**
    * task output description
    */
  val outputConfigDesc: String


  /**
    * Sequence of config keys and their associated values
    */
  def paramConfigDoc: Config


  /**
    *
    * @param component name of the component
    * @return config structure of the task
    */
  final def configStructure(component: String): String = {
    val config = ConfigFactory parseString
         s"""
          | {
          |   ${Keywords.Task.COMPONENT} = $component
          |   ${Keywords.Task.TASK} = $taskName
          | }
          """.stripMargin
    HoconConfigUtil.render(config.withValue(Keywords.Task.PARAMS, paramConfigDoc.root()).root())
  }



}
