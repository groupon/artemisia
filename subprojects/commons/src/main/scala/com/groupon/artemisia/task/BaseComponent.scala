package com.groupon.artemisia.task

import java.util
import scala.collection.JavaConverters._
import com.groupon.artemisia.inventory.exceptions.UnKnownTaskException
import com.groupon.artemisia.util.Util
import com.typesafe.config.Config

/**
  * Created by chlr on 12/22/17.
  */
abstract class BaseComponent(val name: String) {

  /**
    * list of supported task name
    */
  val tasks: util.List[_ <: BaseTaskLike]

  /**
    * field to differentiate Component and JComponent
    */
  val componentType: APIType

  /**
    * default config applicable to all task
    */
  val defaultConfig: Config

  /**
    * one line description of the Component
    */
  val info: String


  /**
    * returns an instance of [[Task]] configured via the config object
    *
    * {{{
    *   dispatch("ScriptTask","mySampleScriptTask",config)
    * }}}
    *
    *
    * @param task task the Component has to execute
    * @param name name assigned to the instance of the task
    * @param config HOCON config payload with configuration data for the task
    * @return an instance of [[Task]]
    */
  final def dispatchTask(task: String, name: String, config: Config): Task = {
    tasks.asScala.find(_.taskName == task) match {
      case Some(x: TaskLike) => x.apply(name, composeDefaultConfig(x, config))
      case Some(x: JTaskLike) => x.create(name, composeDefaultConfig(x, config))
      case _ => throw new UnKnownTaskException(s"unknown task $task in component $name")
    }
  }


  /**
    * compose default task configuration
    * @param task
    * @param config
    * @return
    */
  private final def composeDefaultConfig(task: BaseTaskLike, config: Config): Config = {
    config
      .withFallback(TaskContext.getDefaults(this.name, task.taskName))
      .withFallback(task.defaultConfig)
      .withFallback(defaultConfig)
  }


  /**
    * A brief overview of the components and the tasks it supports.
    */
  final def doc: String = {
    val taskTable: Seq[Array[String]] =  Array("Task", "Description") +: tasks.asScala.map(x => Array(x.taskName, x.info))
    s"""/
       /$name
       /${"=" * name.length}
       /
        /$info
       /
        /${Util.prettyPrintAsciiTable(taskTable.toArray).mkString(System.lineSeparator())}
       /
     """.stripMargin('/')
  }


  /**
    * get documentation of the task
    * @param task name of the task
    */
  def taskDoc(task: String): String = {
    tasks.asScala.find(_.taskName == task) match {
      case Some(x: TaskLike) => x.doc(name)
      case Some(x: JTaskLike) => x.convert.doc(name)
      case Some(_) => throw new UnKnownTaskException(s"The Task is neither a scala task nor a java task")
      case None => throw new UnKnownTaskException(s"unknown task $task in component $name")
    }
  }


}
