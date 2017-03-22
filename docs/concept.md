# Defining Jobs: 
  
## Basics
  
  In Artemisia job definition is declarative and not imperative. The entire job is defined as a Hocon config object.
  It is not pure Hocon but with few mutations. One major extension added is that the quoted values still
  allows substitution. Normal Hocon config object doesn't support substitution for the quoted values.
  Below is an job with an hypothetical task to add two number.  
  
        print_task = {
            Component = HComponent
            Task = HPrintTask
            params = {
               print = "Hello World" 
            }
        }
 
  
  The above is a simple job which has a single task called here as `print_task`. Two important property of a task are
  `Component` and `Task`. `Component` is a container which encapsulates related tasks. for e.g. you have a `MySQLComponent`
  which can have tasks such as
  
  * `SQLLoad` (load data to a mysql table from file)
  * `SQLExport` (export sql query content to file)
  * `SQLExecute` (execute DML queries such as UPDATE/DELETE/INSERT on a mysql table)
  * `SQLRead` (execute query and save result in a variable).
  
  Most `Components` and `Tasks` shown in this page are **hypothetical** and may or may not exist in the actual product.
  
## Defining multiple step jobs:

  The above job definition had a single step in it. But typically a job definition will have multiple steps defined.
  An example multi-step job is shown below.

        create_file = {
            Component = FileComponent
            Task = CreateFile
            params = {
                file = /var/tmp/artemisia/file.txt
                content = "you may fire when ready"
            }
        }
        
        delete_file = {
            Component = FileComponent
            Task = DeleteFile
            dependencies = [ create_file ]
            params = {
                file = /var/tmp/artemisia/file.txt
            }
        }
  
  The above job definition has two tasks *create_file* and *delete_file*. *delete_file* tasks sets up dependency on *create_file* 
  task via its `dependencies` node and thus ensuring *delete_file* task is run only after the successful completion of *create_file*
  task.
  
## Defining variables 

   You can also define variables as shown in the below example with variable `filename`.
   Please note that the resolution and substitution follows Hocon specification except for the fact that quoted values are also resolved.
   For example if normal Hocon `{ foo = "${bar}" }` the value of foo remains as `${bar}` since it is quoted.
   Whereas in the Hocon `{ foo = ${bar}}` the *bar* variable is resolved since it is not quoted.
   In Artemisia both Hocon configs are resolved. ie even though `${bar}` was quoted the in first case it Artemisia would be resolved.

          {
             filename = /var/tmp/artemisia/file.txt
              delete_file = {
                 Component = FileComponent
                 Task = DeleteFile
                 params = {
                    file = ${filename}
                }
           }

## Task Structure

  Below is the common structure of a task expressed as Hocon config.

        step2 = {
            Component = HComponent
            Task = HPrintTask
            dependencies = {
               success = [step1a, step1b]
               fail = [step1c, step1d]
               complete = [step1e, step1f]
            }
              ignore-error = true
              cooldown = 2s
              attempts = 1
              when = 1 > 2
              assert = 1 < 3
              params = {
            }   
            define = {
              foo = bar
              hello = world
            }
            for-all = [
              {key1 = val1},
              {key2 = val2}
            ]
        }
        
  Each nodes in the above config object is explained below
  
#### Component: 
   This selects the component of the task. Components encapsulates similar tasks together. For eg the MySQLComponent
   aggregates tasks that loads, export, queries a MySQL database. similarly you can have components for other databases,
   hadoop, spark etc. The value must be one of the supported component by Artemisia.
     
     
#### Task:
   This field selects a task within the previously selected Component.
   It must be one of the task supported by the previously selected Component.
  
#### dependencies:
   This field sets the upstream *dependencies* of the current node. The *dependencies* field sometimes takes an object as
   an value and this object can have only the below fields 
    
    * success: success dependency path. In the above example step2 task can run only if step1a and step1b completes successfully.
    * fail: failure dependency path. In the above example step2 task can run only if tasks step1c and step1d execution fails.
    * complete: completion dependency path. In the above example step3 can run if step1e and step1f completes irrespective
      of it succeeding or failing.
      
    For tasks which has only success dependency path it can use the shortcut as shown below. In this shortcut mode, the
     dependency field will have value of type array instead of object and this array holds the task list for success 
     dependency path (*dependencies.success*).
            
            task3 = {
              Component = HComponent
              Task = HTask
              dependencies = [task1, task2]
            }
  
#### ignore-error:
   This field takes boolean value (yes, no, true, false). if set to yes the node failure will not stop the entire dag.
   The current node's failure will be ignore and the next node in the dag will be processed.
   
#### attempts:
   This field decides how many times a node must retry execution upon failure. 
        
#### cooldown:
   This field decides how long a node must wait before a retry.
      
#### when:
   This field's value must evaluate as a boolean expression. if the boolean expression evaluates to true the node
   is executed else node's execution is skipped.
       
#### assert:
   This field too sports a boolean expression. But this is evaluated after the task execution has completed and this
   is generally used to assert if the task execution generated the desired result. This expression must evaluate to true
   or false. If `true` then assertion succeeds or if evaluates to `false` the assertion fails and so does the task.
   for instance consider the below hypothetical node
         
        node1 = {
             Component = HMathComponent
             Task = HAdderTask
             params = {
               num1 = 10
               num2 = 20
               output_var = num3
             }
             assert = "${num3} == 30"
             }
          
   In the above snippet we have a hypothetical task takes two parameter *num1* and *num2*. It adds these two parameters
   and assigns the value to a new parameter called num3. In post-execution our assert node confirms if the num3 actually
   evaluates to 30. 

#### params:
   All task specific configuration items goes here. each task will have its unique config object nested inside *params* node.
   

#### define:
   This field is used to define local variables for the task. The scope of the variable defined here is localized to task
   and not available to other tasks. The below step define two local variables *foo* and *bar* which is not available outside
   of this task.
   
             node1 = {
                Component = HMathComponent
                Task = HAdderTask
                params = {
                  num1 = ${foo}
                  num2 = ${bar}
                  output_var = num3
                }
               define = {
                  foo = 10
                  bar = 12
               }
             }
                          
   
#### for-all:
   
   The field is used to create looping in tasks. This creates copies of the task but each with a different set of 
   local variables. for eg the below task 
    
            taskname = {
              Component = HMathComponent
              Task = HAdderTask
              params = {
                num1 = ${foo}
                num2 = ${bar}
              }
              for-all = [
                { foo = 10, bar = 21}
                { foo = 1, bar = 2 }
                { foo = 100, bar = 120 }
              ]
            }
            
   is transformed into the below.
    
            taskname$1 = {
              Component = HMathComponent
              Task = HAdderTask  
              params = {
                num1 = ${foo}
                num2 = ${bar}
              }
              define = {
                foo = 10, bar = 21
              }                                               
            }
            
            taskname$2 = {
              Component = HMathComponent
              Task = HAdderTask
              dependencies = [ taskname$1 ]
              params = {
                num1 = ${foo}
                num2 = ${bar}
              }
              define = {
                foo = 1, bar = 2
              }             
            }
            
            taskname$3 = {
              Component = HMathComponent
              Task = HAdderTask
              dependencies = [ taskname$2 ]
              params = {
                num1 = ${foo}
                num2 = ${bar}
              }
              define = {
                foo = 100, bar = 120
              }             
            }            
  
   The above example shows the simplified form of iterations. In the more configurable version of iterations the *for-all*
   field takes value of type object instead of array. This object can have three fields
        * ignore-failures: each of the expanded node of the iteration is linked by *dependencies.complete* path instead of 
          *dependencies.success* as in the previous case. So even if one of the iteration fails the rest of the iterations
          are executed. By default this is set to *no*.
        * values: actual array of values which is to be iterated
        * group: controls the parallelism in the expanded nodes. for example if we iterate for 10 different values with
          group value set to 2, then we execute nodes in batches of 2 (i.e. two nodes will be executed in parallel) 
          and we will have 5 iterations. 
    
   for eg the below task configuration 
   
               taskname = {
                 Component = HMathComponent
                 Task = HAdderTask
                 params = {
                   num1 = ${foo}
                   num2 = ${bar}
                 }
                 for-all = {
                   ignore-failures = yes 
                   group = 2
                   values = [
                       { foo = 10, bar = 21}
                       { foo = 1, bar = 2 }
                       { foo = 100, bar = 120 }
                    ]
                 }
               }
   
   is expanded as below.
   
            taskname$1 = {
              Component = HMathComponent
              Task = HAdderTask  
              params = {
                num1 = ${foo}
                num2 = ${bar}
              }
              define = {
                foo = 10, bar = 21
              }                                               
            }
            
            taskname$2 = {
              Component = HMathComponent
              Task = HAdderTask
              params = {
                num1 = ${foo}
                num2 = ${bar}
              }
              define = {
                foo = 1, bar = 2
              }             
            }
            
            taskname$3 = {
              Component = HMathComponent
              Task = HAdderTask
              dependencies.complete = [ taskname$1, taskname$2 ]
              params = {
                num1 = ${foo}
                num2 = ${bar}
              }
              define = {
                foo = 100, bar = 120
              }             
            }         
    
   
   In the above example *taskname$1* and *taskname$2* are executed in parallel and *taskname$3* is executed after both
   *taskname$1* and *taskname$2* is completed. if *group* key of the *for-all* node was 3, then all the three nodes
   would have been executed in parallel. Also notice that *taskname$3* dependency is based on completion of *taskname$1*
   and *taskname$2* and not on success or failure. so *taskname$3* will be executed even if either of *taskname$1* or
   *taskname$2* fails.
   
    