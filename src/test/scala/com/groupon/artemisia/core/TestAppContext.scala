package com.groupon.artemisia.core

import com.groupon.artemisia.core.AppContext.{DagSetting, Logging}
import com.groupon.artemisia.task.Component
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

/**
  * Created by chlr on 12/20/17.
  */
class TestAppContext(config: Config) extends AppContext(AppSetting()) {

  override val globalConfigFile = None
  override def getConfigObject: Config = config
  override val logging = Logging("INFO", "INFO")
  override val dagSetting = DagSetting(1, 1, FiniteDuration(1L, "seconds"), FiniteDuration(1L, "seconds"), false, false)
  override val checkpointMgr = new BasicCheckpointManager()
  override val componentMapper: Map[String,Component] = Map()

  override def init(): AppContext = {
    payload = getConfigObject
    this
  }

}
