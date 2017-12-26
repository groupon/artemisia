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

package com.groupon.artemisia.dag

import java.io.File

import com.groupon.artemisia.core.BasicCheckpointManager.CheckpointData
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.dag.Dag.Node
import com.typesafe.config._
import com.groupon.artemisia.util.HoconConfigUtil.{Handler, configToConfigEnhancer}
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
import scala.util.{Failure, Success, Try}
/**
  * Created by chlr on 8/12/16.
  */

object DagEditor {

  /**
    * edit dag and emit new nodes and associated config object which will be later merged with the jobConfig.
    * ideally the jobConfig object can be inferred from new nodes objects
    * because of the payload property of the node object. But yet this method
    * outputs both new nodes generated and the jobConfig because the jobConfig
    * could have additional properties than the new nodes. for instance in
    * worklet import the jobConfig could have additional variables defined in the
    * imported module which can be merged with jobConfig with this design.
    *
    * @param node
    * @param jobConfig
    * @return
    */
  def editDag(node: Node, jobConfig: Config): (Seq[Node],Config) = {
    node match {
      case x if isIterableNode(x) => new NodeIterationProcessor(node, jobConfig).expand
      case x if isWorkletNode(x) => new ModuleImporter(node, jobConfig).importModule
      case _ => throw new DagEditException(node)
    }
  }


  /**
    * inspects and confirms if a node requires editing.
    * editing could be such as
    *  * expanding iterable nodes
    *  * importing worklets
    *
    * @param node node to be inspected
    * @return boolean value to indicate result
    */
  def requireEditing(node: Node) = isIterableNode(node) || isWorkletNode(node)


  /**
    * is node iterable
    *
    * @param node input node
    * @return
    */
  private def isIterableNode(node: Node) = node.payload.hasPath(Keywords.Task.ITERATE)


  /**
    * is node a worklet import node
    * @param node input node
    * @return
    */
  private def isWorkletNode(node: Node) = {
    (node.payload.getString(Keywords.Task.COMPONENT) == Keywords.DagEditor.Component) &&
      (node.payload.getString(Keywords.Task.TASK) == Keywords.DagEditor.Task)
  }


  /**
    * update the graph of the Dag by replacing editable Dag node with
    * expanded nodes. apply checkpoints on the new generated nodes if
    * checkpoints exists.
    *
    * @param dag Dag to be updated.
    * @param node node to replaced
    * @param nodes new nodes replacing the node
    */
  def replaceNode(dag: Dag ,node: Node, nodes: Seq[Node], checkpoint: CheckpointData) = {
    nodes.filter(_.allParents == Nil )
      .foreach({_.setDependencies(node.successParents, node.failParents, node.completeParents)})
    val leafNodes = nodes.filterNot(x => nodes.exists(_.allParents contains x))
    dag.graph.filter(_.successParents contains node)
      .foreach(x => x.successParents = x.successParents.filterNot(_ == node) ++ leafNodes)
    dag.graph.filter( _.failParents.contains(node))
      .foreach(x => x.failParents = x.failParents.filterNot(_ == node) ++ leafNodes)
    dag.graph.filter(_.completeParents contains node)
      .foreach(x => x.completeParents = x.completeParents.filterNot(_ == node) ++ leafNodes)
    dag.graph = dag.graph.filterNot(_ == node) ++ nodes
    nodes.foreach(
      x => checkpoint.taskStatRepo.get(x.name) match {
        case Some(taskStat) => dag.setNodeStatus(x.name,taskStat.status)
        case _ => ()
      }
    )
  }


  /**
    * Inject imported module into Dag
    * @param node
    * @param referenceConfig
    */
  private[dag] class ModuleImporter(node: Node, referenceConfig: Config) {

    val inline = "inline"
    val file = "file"

    /**
      * process imported worklet node. The following changes are done
      *   * change node dependencies with its modified new name
      *   * change node settings with the settings inherited from the import node
      * @param importedNodePayLoad
      * @param parentNode
      * @return
      */
    private def processImportedNode(importedNodePayLoad: Config, parentNode: Node) = {
      var result = importedNodePayLoad.getAs[ConfigValue](Keywords.Task.DEPENDENCY).map(_.valueType()) match {
        case Some(ConfigValueType.LIST) =>
          importedNodePayLoad.withValue(Keywords.Task.DEPENDENCY, ConfigValueFactory.fromIterable(
            importedNodePayLoad.as[List[String]](Keywords.Task.DEPENDENCY).map(x => s"${parentNode.name}$$$x").asJava))
        case Some(ConfigValueType.OBJECT) =>
          importedNodePayLoad.withValue(Keywords.Task.DEPENDENCY, Seq(Keywords.Task.COMPLETE_DEPENDENCY,
            Keywords.Task.FAIL_DEPENDENCY, Keywords.Task.SUCCESS_DEPENDENCY).foldLeft(ConfigFactory.empty)({
            case (acc, item) => acc.withValue(item, ConfigValueFactory.fromIterable(
              parentNode.payload.as[List[String]](s"${Keywords.Task.DEPENDENCY}.$item")
                .map(x => s"${parentNode.name}$$$x").asJava))}).root())
        case Some(_) => throw new DagException(s"the ${Keywords.Task.DEPENDENCY} must either be a list or object. " +
          s"any other type is not supported")
        case None => importedNodePayLoad
      }
      // Assertion is not added here because the assertion node must be added only in the last node(s) of the  Worklet
      val taskSettingNodes = Seq(Keywords.Task.IGNORE_ERROR, Keywords.Task.COOLDOWN, Keywords.Task.ATTEMPT,
        Keywords.Task.CONDITION, Keywords.Task.ASSERTION)
      result = taskSettingNodes.foldLeft(result) {
        (config: Config, inputNode: String) => parentNode.payload.getAs[ConfigValue](inputNode)
          .map{x => config.withValue(inputNode, x)}
          .getOrElse(config)
      }
      // merge variables of both nodes.
      result.withValue(Keywords.Task.VARIABLES, result.getAs[Config](Keywords.Task.VARIABLES) ->
        parentNode.payload.getAs[Config](Keywords.Task.VARIABLES) match {
        case (Some(x), Some(y)) => y.withFallback(x).root
        case (Some(x), None) => x.root
        case (None, Some(x)) => x.root
        case (None,None) => ConfigFactory.empty.root
      })
    }


    private def moduleConfig: Config = {
      if (node.payload.hasPath(s"${Keywords.Task.PARAMS}.$file")) {
        val fileName = node.payload.getConfig(Keywords.Task.PARAMS).root.keySet.asScala.filterNot(x => x == file)
          .foldLeft(node.payload.as[Config](Keywords.Task.PARAMS))({case (acc, key) => acc.withoutPath(key)})
          .hardResolve(referenceConfig)
          .as[String](file)
        ConfigFactory.parseFile(new File(fileName))
      }
      else if (node.payload.hasPath(s"${Keywords.Task.PARAMS}.$inline")) {
        val worklet = node.payload.getConfig(Keywords.Task.PARAMS).root.keySet.asScala.filterNot(x => x == inline)
          .foldLeft(node.payload.as[Config](Keywords.Task.PARAMS))({case (acc, key) => acc.withoutPath(key)})
          .hardResolve(referenceConfig)
          .as[String](inline)
        referenceConfig.getConfig(Keywords.Config.WORKLET).as[Config](worklet)
      }
      else {
        throw new IllegalArgumentException(s"$file or $inline field is expected for Import module")
      }
    }


    /**
      * import worklet module and prepare it for integration with main job
      * @return
      */
    def importModule: (Seq[Node], Config) = {
      var module: Config = moduleConfig
      val nodeMap = Dag.extractTaskNodes(module)
      val nodes = nodeMap.toSeq map {
        case (name, payload) =>
          // performing side-effect in map operation.
          module = module
            .withoutPath(name)
            .withValue(s""""${node.name}$$$name"""", Try(processImportedNode(payload, node).root()) match {
              case Success(x) => x
              case Failure(th) => th.printStackTrace(System.err); throw th
            })
          Node(s"${node.name}$$$name", module.as[Config](s""""${node.name}$$$name""""))
      }
      val dag = Dag(nodes) // we create a dag object to link nodes and identify cycles.
      node.payload.getAs[ConfigValue](Keywords.Task.ASSERTION) foreach {
        assertion => dag.leafNodes.foreach(x => x.payload.withValue(Keywords.Task.ASSERTION, assertion))
      }
      dag.graph -> module
    }

  }


  /**
    * this class processes a single node and expands it to several nodes based on its iteration logic
    * @param node
    */
  private[dag] class NodeIterationProcessor(node: Node, referenceConfig: Config) {

    protected def iterationValue: (ConfigValue, Int, Boolean) = {
      val iterateValue: ConfigValue = node.payload.root.keySet.asScala.filterNot(_ == Keywords.Task.ITERATE)
        .foldLeft(node.payload)({ case (acc, key) => acc.withoutPath(key)})
        .hardResolve(node.payload.getAs[Config](Keywords.Task.VARIABLES)
          .getOrElse(ConfigFactory.empty()).withFallback(referenceConfig)).as[ConfigValue](Keywords.Task.ITERATE)
      iterateValue.valueType match {
        case ConfigValueType.STRING => (iterateValue, 1, false)
        case ConfigValueType.LIST => (iterateValue, 1, false)
        case ConfigValueType.OBJECT =>
          val x = iterateValue.asInstanceOf[ConfigObject].toConfig
          (x.as[ConfigValue]("values"),
            x.getAs[Int]("group").getOrElse(1),
            x.getAs[Boolean]("allow-failure").getOrElse(false))
        case _ => throw new RuntimeException(s"invalid config for ${Keywords.Task.ITERATE} for node ${node.name}")
      }
    }

    /**
      * evaluate expression transform result to Traversable of Config
      * @param expr
      * @return
      */
    protected def exprEvaluator(expr: String): Traversable[Config] = {
      val toolbox = universe.runtimeMirror(this.getClass.getClassLoader).mkToolBox()
      Try(toolbox.compile(toolbox.parse(expr))) match {
        case Success(_) => ()
        case Failure(_) => throw new DagException(s"unable to compile scala expression: $expr")
      }
      toolbox.eval(toolbox.parse(expr)) match {
        case x: Traversable[Map[String, Any]] @unchecked => x.map(x => x.foldLeft(ConfigFactory.empty)({
          case (acc, (key, value)) => acc.withValue(key, ConfigValueFactory.fromAnyRef(value))
        }))
        case _ => throw new DagException(s"The compiled expression $expr didn't return an acceptable type. " +
          s"The type must be Traversable[Map[String,T]] where T is any valid type for" +
          " `com.typesafe.config.ConfigValueFactory.fromAnyRef`")
      }
    }

    /**
      *
      * @return
      */
    def expand: (Seq[Node], Config) = {
      val (values: ConfigValue,  groupSize: Int, allowFailure: Boolean) = iterationValue
      val valueList: Vector[ConfigValue] = values.valueType() match {
        case ConfigValueType.STRING => exprEvaluator(values.unwrapped().toString).map(_.root()).toVector
        case ConfigValueType.LIST => values.asInstanceOf[ConfigList].asScala.toVector
        case _ => throw new DagException(s"iteration values (${values.render(ConfigRenderOptions.concise)}) can " +
          s"either be a String or List only")
      }
      val nodes = for (i <- 1 to valueList.size) yield {
        Node(s"${node.name}$$$i",
          node.payload.withoutPath(Keywords.Task.ITERATE)
            .withValue(Keywords.Task.VARIABLES, valueList(i-1)
              .withFallback(node.payload.getAs[Config](Keywords.Task.VARIABLES).getOrElse(ConfigFactory.empty)))
        )
      }
      nodes.sliding(groupSize,groupSize).sliding(2,1) foreach {
        case parents :: children :: Nil => for(child <- children) {
          if (allowFailure) child.completeParents =  parents else child.successParents = parents
        }
        case _ :: Nil => ()
      }
      val outputConfig = nodes.foldLeft(ConfigFactory.empty) {
        case (carry, inputNode) => carry.withFallback(ConfigFactory.empty.withValue(s""""${inputNode.name}"""",
          inputNode.payload.root()))
      }
      nodes -> outputConfig
    }
  }

}






