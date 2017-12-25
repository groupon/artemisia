package com.groupon.artemisia

import com.typesafe.config.Config

/**
  * Created by chlr on 12/22/17.
  */
package object task {

  implicit class ComponentConverter(jComponent: JComponent) {
    def convert: Component = {
      new Component(jComponent.name) {
        override val defaultConfig: Config = jComponent.defaultConfig
        override val tasks: java.util.List[_ <: BaseTaskLike] = jComponent.tasks
        override val info: String = jComponent.info
      }
    }
  }

  implicit class TaskConverter(jTaskLike: JTaskLike) {
    def convert: TaskLike = {
      new TaskLike {
        override def apply(name: String, config: Config, reference: Config): Task =
          jTaskLike.create(name, config, reference)
        override def fieldDefinition: Map[String, AnyRef] = Map() // TODO implemetation
        override def defaultConfig: Config = jTaskLike.defaultConfig
        override val taskName: String = jTaskLike.taskName
        override def paramConfigDoc: Config = jTaskLike.paramConfigDoc
        override val outputConfig: Option[Config] = if (jTaskLike.outputConfig.isPresent)
          Some(jTaskLike.outputConfig().get()) else None
        override val outputConfigDesc: String = jTaskLike.outputConfigDesc
        override val desc: String = jTaskLike.desc
        override val info: String = jTaskLike.info
      }
    }
  }


}
