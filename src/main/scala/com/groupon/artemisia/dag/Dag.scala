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

import com.typesafe.config._
import com.groupon.artemisia.core.AppLogger._
import com.groupon.artemisia.core.BasicCheckpointManager.CheckpointData
import com.groupon.artemisia.core.Keywords.Task
import com.groupon.artemisia.core._
import com.groupon.artemisia.dag.Dag.Node
import com.groupon.artemisia.task.{TaskConfig, TaskHandler}
import com.groupon.artemisia.util.HoconConfigUtil.{Handler, configToConfigEnhancer}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.util.{Success, Try, Failure}



/**
  * Created by chlr on 1/3/16.
  */

private[dag] class Dag(var graph: Seq[Node], checkpointData: CheckpointData) {

  this.resolveNodeDependency()
  debug("resolved all task dependency")
  graph = topSort(graph)
  debug("no cyclic dependency detected")

  /**
    * assign config payload to each Node
    *
    * @param code
    */
  def updateNodePayloads(code: Config) = {
    Dag.extractTaskNodes(code) foreach {
      case (node_name, payload) => this.lookupNodeByName(node_name).payload = payload
    }
  }

  /**
    * Edits the current DAG when runnable nodes have to be edited.
    * editing of nodes are done lazily.
    *
    * @param code application config payload
    * @return
    */
  @tailrec private def editDag(code: Config): Config = {
    this.getRunnableNodes.filter(DagEditor.requireEditing) match {
      case Nil => code
      case nodes =>
        editDag(nodes.foldLeft(code) {
          (carry, node) =>
            val (newNodes, newConfig) =  DagEditor.editDag(node, code)
            DagEditor.replaceNode(this, node, newNodes, checkpointData) // performing side-effects inside a map function.
            newConfig withFallback carry.withoutPath(s""""${node.name}"""")
        })
    }
  }


  /**
    * get status of the node
    *
    * @param nodeName node name
    * @return
    */
  def getNodeStatus(nodeName: String) = lookupNodeByName(nodeName)._status

  /**
    * set node status and appropriately deactive any downstream nodes if necassary
    *
    * @param nodeName
    * @param status
    */
  def setNodeStatus(nodeName: String, status: Status.Value): Unit = {
    val node = this.lookupNodeByName(nodeName)
    assert(!Status.isTerminal(node._status), s"node $nodeName with status ${node._status} cannot be set with new status $status")
    info(s"setting node $nodeName status to $status")
    node._status = status
    status match {
      case Status.DEACTIVATE => this.allChildren(node).foreach(x => setNodeStatus(x.name, Status.DEACTIVATE))
      case Status.FAILED_RECOVERED => successPathChildren(node).foreach(y => setNodeStatus(y.name, Status.DEACTIVATE))
      case x if Status.isSuccessful(x) => failurePathChildren(node).foreach(y => setNodeStatus(y.name, Status.DEACTIVATE))
      case _ => ()
    }
  }


  /**
    * generate reference config for given node. this is a Hocon config with every other nodes name as key
    * and its payload as its value.
    *
    * @param jobPayload
    * @return
    */
  def nodeReferenceConfig(jobPayload: Config, node: Node): Config = {
    graph.filterNot(_ == node).foldLeft(ConfigFactory.empty)({
      case (acc, inputNode) => acc.withValue(inputNode.name, jobPayload.as[ConfigValue](inputNode.name))
    })
  }

  /**
    * get sequence of tasks whose dependencies have met.
    *
    * @param appContext
    * @return
    */
  def getRunnableTasks(appContext: AppContext): Try[Seq[(String, Try[TaskHandler])]] = {
    def getTasks = for (node <- this.getRunnableNodes) yield {
      node.name -> Try(node.getNodeTask(nodeReferenceConfig(appContext.payload,node), appContext))
    }
    Try(editDag(appContext.payload)) match {
      case Success(x) => appContext.payload = x; Try(getTasks)
      case Failure(th: DagEditException) => Success(Seq((th.node.name, Failure(th))))
      case Failure(th) => Failure(th)
    }
  }


  /**
    * leaf nodes of the graph.
    * leaf nodes are the nodes who dont have any children nodes
    */
  def leafNodes = graph filterNot (x => graph.exists(_.allParents contains x))


  /**
    * get the sequence of nodes whose dependencies are fully met
    *
    * @return
    */
  def getRunnableNodes: Seq[Node] = {
    graph.map(x => checkpointData.taskStatRepo.get(x.name) match {
      case Some(stat) if x._status != stat.status => x._status = stat.status; x
      case _ => x
    }).filter(_.isRunnable)
  }

  /**
    * check if node is recoverable.
    * A recoverable node is a node which has a child linked
    * either by failure or complete status
    *
    * @param nodeName
    * @return
    */
  def isRecoverableNode(nodeName: String): Boolean = {
    graph exists {
      x => x.failParents.contains(lookupNodeByName(nodeName)) ||
        x.completeParents.contains(lookupNodeByName(nodeName))
    }
  }

  /**
    * check if all nodes in the dag has been processed
    *
    * @return
    */
  def hasCompleted = graph forall { _.isFinished }

  /**
    * running nodes
    *
    * @return
    */
  def runningNodes = graph filter { _._status == Status.RUNNING }

  /**
    * failed nodes
    *
    * @return
    */
  def failedNodes = graph filter { x => Status.isFail(x._status) }

  /**
    * succeeded nodes
    *
    * @return
    */
  def succeededNodes = graph filter { x => Status.isSuccessful(x._status) }


  /**
    * lookup nodes with specific status
    *
    * @param status
    * @return
    */
  def nodesWithStatus(status: Status.Value) = graph filter { _._status == status }

  /**
    * completed nodes
    *
    * @return
    */
  def completedNodes = graph filter { x => Status.isComplete(x._status) }

  /**
    * downstream nodes that are in the success path
    *
    * @param node
    * @return
    */
  def successPathChildren(node: Node) = graph filter { x => x.successParents contains node }

  /**
    * downstream nodes that are in the fail path
    *
    * @param node
    * @return
    */
  def failurePathChildren(node: Node) = graph filter { x => x.failParents contains node }

  /**
    * downstream nodes that are in the complete path
    *
    * @param node
    * @return
    */
  def completePathChildren(node: Node) = graph filter { x => x.completeParents contains node }

  /**
    * get all children nodes
    *
    * @param node
    * @return
    */
  def allChildren(node: Node) = graph filter { x => x.allParents contains node }

  override def toString = graph.toString()

  /**
    * resolve dependencies in the graph
    */
  protected def resolveNodeDependency() = {
    for (node <- graph) {
      node.parseDependencyFromConfig match {
        case (successList, failList, completeList) =>
          node.successParents = successList.map(lookupNodeByName)
          node.failParents = failList.map(lookupNodeByName)
          node.completeParents = completeList.map(lookupNodeByName)
      }
    }
  }

  /**
    * lookup node by name
    *
    * @param nodeName
    * @return
    */
  def lookupNodeByName(nodeName: String): Node = {
    val nodeMap = graph.map(x => x.name -> x).toMap
    nodeMap.lift(nodeName) match {
      case Some(x) => x
      case None => throw new DagException(s"invalid dependency reference. $nodeName not found")
    }
  }

  /**
    * perform a top-sort on the Graph to validate acylic property of the Graph.
    *
    * @param unsorted_graph
    * @param sorted_graph
    * @return
    */
  @tailrec
  private def topSort(unsorted_graph: Seq[Node], sorted_graph: Seq[Node] = Nil): Seq[Node] = {
    lazy val openNodes = unsorted_graph collect {
      case node@Node(_, Nil) => node
      case node@Node(_, parents) if parents forall {
        sorted_graph.contains
      } => node
    }
    (unsorted_graph, sorted_graph) match {
      case (Nil, a) => a
      case _ if openNodes.isEmpty =>
        AppLogger error s"cyclic dependency detected in graph structure $unsorted_graph"
        throw new DagException("Cycles Detected in Dag")
      case _ => topSort(unsorted_graph filterNot {
        openNodes.contains
      }, sorted_graph ++ openNodes)
    }
  }
}

object Dag {

  def apply(appContext: AppContext): Dag = {
    val node_list = extractTaskNodes(appContext.payload) map {
      case (name, payload) => Node(name, payload)
    }
    new Dag(node_list.toList, appContext.checkpoints)
  }


  /**
    * This method parses a given Config Object and identifies task definition nodes and their corresponding node payload.
    *
    *
    * @param config config payload to be parsed
    * @return Map of taskname and task definition config objects
    */
  private[dag] def extractTaskNodes(config: Config): Map[String, Config] = {
    val nodeNames = config.root().asScala
      .filterNot({case (key, _) => key.startsWith("__") && key.endsWith("__")})
      .filter({case (_, value) => value.valueType() == ConfigValueType.OBJECT})
      .filter({case (_, value: ConfigObject) => value.toConfig.hasPath(Task.COMPONENT) &&
        value.toConfig.hasPath(Keywords.Task.TASK)})
      .map({ case (key, _) => key})
    nodeNames
      .map(x => x -> nodeNames.filterNot(_ == x).foldLeft(config)({ case (acc, node) => acc.withoutPath(node)}))
      .toMap
  }

  def apply(node_list: Seq[Node], checkpointData: CheckpointData = CheckpointData()) = {
    new Dag(node_list, checkpointData)
  }


  class Node(val name: String, var payload: Config) {

    val ignoreFailure: Boolean = false

    private[Dag] var _status = Status.READY

    private var _success, _fail, _complete: Seq[Node] = Nil

    /**
      * read node status
      * @return
      */
    def status = _status

    /**
      * combined dependency of the node
      *
      * @return
      */
    def allParents = successParents ++ failParents ++ completeParents

    /**
      * getter for successful parents
      *
      * @return
      */
    def successParents = _success

    /**
      * setter for success dependency attribute and as side-effect operation
      * update the payload with adjusted config for the dependency node
      *
      * @param nodes new parents for the
      */
    def successParents_=(nodes: Seq[Node]) = {
      payload = ConfigFactory.empty().withValue(s"${Keywords.Task.DEPENDENCY}.${Keywords.Task.SUCCESS_DEPENDENCY}",
        ConfigValueFactory.fromIterable(nodes.map(_.name).asJava)) withFallback payload
      _success = nodes
    }

    /**
      * update all dependencies for the node
      *
      * @param successParents
      * @param failParents
      * @param completeParents
      */
    def setDependencies (successParents: Seq[Node], failParents: Seq[Node], completeParents: Seq[Node]) = {
      this.successParents = successParents
      this.failParents = failParents
      this.completeParents = completeParents
    }

    /**
      * getter for failed parents
      *
      * @return
      */
    def failParents = _fail

    /**
      * setter for parents attribute and as side-effect operation
      * update the payload with adjusted config for the dependency node
      *
      * @param nodes new parents for the
      */
    def failParents_=(nodes: Seq[Node]) = {
      payload = ConfigFactory.empty().withValue(s"${Keywords.Task.DEPENDENCY}.${Keywords.Task.FAIL_DEPENDENCY}",
        ConfigValueFactory.fromIterable(nodes.map(_.name).asJava)) withFallback payload
      _fail = nodes
    }

    /**
      * getter for completed parents
      *
      * @return
      */
    def completeParents = _complete

    /**
      * setter for parents attribute and as side-effect operation
      * update the payload with adjusted config for the dependency node
      *
      * @param nodes new parents for the
      */
    def completeParents_=(nodes: Seq[Node]) = {
      payload = ConfigFactory.empty().withValue(s"${Keywords.Task.DEPENDENCY}.${Keywords.Task.COMPLETE_DEPENDENCY}",
        ConfigValueFactory.fromIterable(nodes.map(_.name).asJava)) withFallback payload
      _complete = nodes
    }

    /**
      * define Node equality.
      * Node equality depends only on the name attribute
      *
      * @param that
      * @return
      */
    override def equals(that: Any): Boolean = {
      that match {
        case that: Node => that.name == this.name
        case _ => false
      }
    }

    override def toString = name

    /**
      * parse dependency config of the node and retrieve the list
      * of dependent nodes for success, fail and complete conditions.
      *
      * @return
      */
    protected[Dag] def parseDependencyFromConfig: (Seq[String], Seq[String], Seq[String]) = {
      val dependencyFields = Seq(Keywords.Task.SUCCESS_DEPENDENCY, Keywords.Task.FAIL_DEPENDENCY,
        Keywords.Task.COMPLETE_DEPENDENCY)
      payload.getAs[ConfigValue](Keywords.Task.DEPENDENCY) match {
        case None => (Nil, Nil, Nil)
        case Some(x) if x.valueType() == ConfigValueType.LIST =>
          (payload.as[List[String]](Keywords.Task.DEPENDENCY), Nil, Nil)
        case Some(x) if x.valueType() == ConfigValueType.OBJECT =>
          val rootConfig = payload.as[Config](Keywords.Task.DEPENDENCY)
          require(rootConfig.root.keySet.asScala.forall(dependencyFields.contains),
            s"dependency field object can have only the following fields: ${dependencyFields.mkString}")
          (rootConfig.getAs[List[String]](Keywords.Task.SUCCESS_DEPENDENCY).getOrElse(Nil),
            rootConfig.getAs[List[String]](Keywords.Task.FAIL_DEPENDENCY).getOrElse(Nil),
            rootConfig.getAs[List[String]](Keywords.Task.COMPLETE_DEPENDENCY).getOrElse(Nil))
      }
    }

    /**
      *
      * @return true if the node is in runnable state.
      */
    def isRunnable = {
      // forall is always true for an empty list
      (successParents forall { x => Status.isSuccessful(x._status) }) &&
        (failParents forall { _._status == Status.FAILED_RECOVERED}) &&
        (completeParents forall { x => Status.isComplete(x._status) }) &&
        this._status == Status.READY
    }

    /**
      *
      * @return
      */
    private[Dag] def isFinished = {
      Status.isTerminal(_status)
    }

    /**
      *
      * @param referenceConfig
      * @param appContext
      * @return
      */
    private[Dag] def getNodeTask(referenceConfig: Config, appContext: AppContext): TaskHandler = {
      this.resolve(referenceConfig)
      val componentName = this.payload.as[String](Task.COMPONENT)
      val taskName = this.payload.as[String](Keywords.Task.TASK)
      val defaults = appContext.payload.getAs[Config](s""""${Keywords.Config.DEFAULTS}"."$componentName"."$taskName"""")
      val component = appContext.componentMapper(componentName)
      val task = component.dispatchTask(taskName, name, this.payload.as[Config](Keywords.Task.PARAMS) withFallback
        defaults.getOrElse(ConfigFactory.empty()))
      new TaskHandler(TaskConfig(this.payload, appContext), task, referenceConfig)
    }


    /**
      * generate resolved payload for the node.
      * The entire node payload is resolved except the assertion field of the node.
      * The assertion field will have to resolved after the task is complete and it
      * emitted its config out
      *
      * @param referenceConfig contextConfig for this node.
      * @return resolved payload of the node.
      */
    private[dag] def resolve(referenceConfig: Config) = {
      // we do this so that assertions are not resolved now and only after task execution completes
      val assertions = payload.as[Config](name).getAs[ConfigValue](Keywords.Task.ASSERTION)
      val variables = payload.as[Config](name).getAs[Config](Keywords.Task.VARIABLES)
        .getOrElse(ConfigFactory.empty())
      // for predictably and to generate clean config
      // we exclude any special nodes like __worklet__, __defaults__ during resolution.
      val config = payload.as[Config](name)
        .withoutPath(Keywords.Task.ASSERTION)
        .withoutPath(Keywords.Task.VARIABLES)
        .hardResolve(variables withFallback referenceConfig)
      this.payload = assertions match {
        case Some(x) => config.withValue(Keywords.Task.ASSERTION, x)
        case None => config
      }
    }

  }

  object Node {

    def apply(name: String, body: Config) = {
      new Node(name, body)
    }

    def apply(name: String) = {
      new Node(name, ConfigFactory.empty())
    }

    def unapply(node: Node) = {
      Some(node.name, node.allParents)
    }

  }

}


