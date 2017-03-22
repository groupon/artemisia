

Dag features
============

* support for task local variables
 
* support for iterations

* support for task imports

* support for worklets


Dependency Features
====================

* dependencies are conditional based on task execution status such as success, fail, complete.

* conditional dependencies are normalized to normal dependencies during top-sort to identify cycles in the DAG

* Nodes which are in the dependency lineage that has not qualified for execution will have their status set to DEACTIVATED.
 
* if Node A1 defines dependency on Node A2 for status `complete`. Then the Node A1 would execute even if the Node 

* there are two types of failure. INIT_FAILED and FAILED. The former occurs when a task is improperly configured and its
  initialization fails (for eg: task missing mandatory parameters), the latter occurs when the task execution fails. 
  
  


Random
=======

* check job restart from checkpoint and if all nodes are properly reconfigured