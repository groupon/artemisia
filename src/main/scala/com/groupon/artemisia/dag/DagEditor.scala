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
import com.typesafe.config._
import com.groupon.artemisia.core.BasicCheckpointManager.CheckpointData
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import Dag.Node
import scala.collection.JavaConverters._
/**
  * Created by chlr on 8/12/16.
  */

object DagEditor {

  /**
    * editdag and emit new nodes and associated config object which will be later merged with the jobConfig.
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
      case x if isIterableNode(x) => expandIterableNode(x)
      case x if isWorkletNode(x) => importModule(x, jobConfig)
      case _ => throw new DagException(s"failed to expand/edit node ${node.name}")
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
    * expand iterable node to sequence of node
    *
    * @param node node to be expanded
    * @return sequence of the expanded nodes
    */
  def expandIterableNode(node: Node) = {
    val (configList: ConfigList, groupSize: Int, allowFailure: Boolean) =
      node.payload.as[ConfigValue](Keywords.Task.ITERATE) match {
      case x: ConfigList => (x, 1, false)
      case x: ConfigObject => (x.toConfig.getList("values"), x.toConfig.getAs[Int]("group").getOrElse(1),
        x.toConfig.getAs[Boolean]("allow-failure").getOrElse(false))
      case _ => throw new RuntimeException(s"invalid config for ${Keywords.Task.ITERATE} for node ${node.name}")
    }
    val nodes = for (i <- 1 to configList.size) yield {
      Node(s"${node.name}$$$i",
        node.payload.withoutPath(Keywords.Task.ITERATE).withValue(Keywords.Task.VARIABLES, configList.get(i - 1))
      )
    }
    nodes.sliding(groupSize,groupSize).sliding(2,1) foreach {
      case parents :: children :: Nil => for(child <- children) {
        if (allowFailure)
          child.completeParents =  parents
        else
        child.successParents = parents
      }
    }
    val outputConfig = nodes.foldLeft(ConfigFactory.empty) {
      case (carry, inputNode) => carry.withFallback(ConfigFactory.empty.withValue(s""""${inputNode.name}"""",
        inputNode.payload.root()))
    }
    nodes -> outputConfig
  }


  /**
    * import worklet module and prepare it for integration with main job
    * @param node
    * @param jobConfig
    * @return
    */
  def importModule(node: Node, jobConfig: Config) = {
    var module = node.payload.as[Config](Keywords.Task.PARAMS).root()
      .keySet().asScala.toList match {
      case "file" :: Nil => ConfigFactory parseFile new File(node.payload.as[String](s"${Keywords.Task.PARAMS}.file"))
      case "inline" :: Nil =>
        jobConfig.getConfig(Keywords.Config.WORKLET).as[Config](node.payload.as[String](s"${Keywords.Task.PARAMS}.inline"))
      case _ => throw new IllegalArgumentException("file and inline are the only supported nodes for Dag Import task")
    }
    val nodeMap = Dag.extractTaskNodes(module)
    val nodes = nodeMap.toSeq map {
      case (name, payload) =>
        // performing side-effect in map operation.
        module = module
          .withoutPath(name)
          .withValue(s""""${node.name}$$$name"""", processImportedNode(payload, node).root())
        Node(s"${node.name}$$$name", module.as[Config](s""""${node.name}$$$name""""))
    }
    val dag = Dag(nodes) // we create a dag object to link nodes and identify cycles.
    node.payload.getAs[ConfigValue](Keywords.Task.ASSERTION) foreach {
      assertion => dag.leafNodes.foreach(x => x.payload.withValue(Keywords.Task.ASSERTION, assertion))
    }
    dag.graph -> module
  }


  /**
    * process imported worklet node. The following changes are done
    *   * change node dependencies with its modified new name
    *   * change node settings with the settings inherited from the import node
    * @param importedNodePayLoad
    * @param parentNode
    * @return
    */
  private def processImportedNode(importedNodePayLoad: Config, parentNode: Node) = {
    val result = importedNodePayLoad.getAs[List[String]](Keywords.Task.DEPENDENCY) match {
      case Some(dependency) => importedNodePayLoad.withValue(Keywords.Task.DEPENDENCY
        ,ConfigValueFactory.fromIterable(dependency.map(x => s"${parentNode.name}$$$x").asJava))
      case None => importedNodePayLoad
    }
    // Assertion is not added here because the assertion node must be added only in the last node(s) of the  worklet
    val taskSettingNodes = Seq(Keywords.Task.IGNORE_ERROR, Keywords.Task.COOLDOWN, Keywords.Task.ATTEMPT, Keywords.Task.CONDITION
      ,Keywords.Task.VARIABLES, Keywords.Task.ASSERTION)
    taskSettingNodes.foldLeft(result) {
      (config: Config, inputNode: String) => parentNode.payload.getAs[ConfigValue](inputNode)
        .map{x => config.withValue(inputNode, x)}
        .getOrElse(config)
    }
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
    nodes foreach {
      x => checkpoint.taskStatRepo.get(x.name) match {
        case Some(taskStat) => dag.setNodeStatus(node.name,taskStat.status)
        case _ => ()
      }
    }
    nodes filter (_.allParents == Nil ) foreach {
      _.setDependencies(node.successParents, node.failParents, node.completeParents)
    }
    val leafNodes = nodes filterNot { x => nodes.exists(_.allParents contains x) }
    dag.graph filter { _.successParents contains node } foreach {
      case x => x.successParents = x.successParents.filterNot(_ == node) ++ leafNodes
    }
    dag.graph filter { _.failParents contains node } foreach {
      case x => x.failParents = x.failParents.filterNot(_ == node) ++ leafNodes
    }
    dag.graph filter { _.completeParents contains node } foreach {
      case x => x.completeParents = x.completeParents.filterNot(_ == node) ++ leafNodes
    }
    dag.graph = dag.graph.filterNot(_ == node) ++ nodes
  }

}
