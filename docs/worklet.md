
Worklet
=======

Artemisia supports worklet in a Dag. Worklets are mini-workflows (reusable dag) that can be referenced and imported into your main
Dag (workflow). There are two modes by which a worklet can imported


#### File Mode:

  In this mode we define the worklet in a separate file and we import the worklet by referencing via their file path.
  For eg the below job definition 
  
        Main Dag definition (main.conf)                       |    	  Worklet Dag definition (worklet.conf)
       =================================                      |    	  =====================================
                                                              |
            tango = 10                                        |    	     bravo = 100
                                                              |
            task1 = {                                         |    	     step1 = {
              Component = TestComponent                       |    	       Component: TestComponent
              Task = TestAdderTask                            |    	       Task: TestAdderTask
              params = {                                      |    	       params = {
                num1 = 1                                      |    	         num1 = 10,
                num2 = 2                                      |    	         num2 = 20,
              }                                               |    	       }
            }                                                 |    	     }
                                                              |
            task2 = {                                         |    	    step2 = {
              Component = DagEditor                           |    	       Component: TestComponent
              Task = Import                                   |    	       Task: TestAdderTask
              dependencies = [task1]                          |    	       dependencies = [ step1 ]
              params = {                                      |    	       params = {
                file = worklet.conf                           |    	         num1 = 30,
              }                                               |    	         num2 = ${tango}
            }                                                 |    	       }
                                                              |    	     }
            task3 = {                                         |
              Component = TestComponent                       |
              Task = TestAdderTask                            |
              dependencies = [task2]                          |
              params = {                                      |
                num1 = ${bravo}                               |
                num2 = 2                                      |
              }                                               |
            }                                                 |
    
  The above code (main.conf) references a worklet (worklet.conf). The resulting DAG is as shown below. Basically the tasks
  and variables in worklet.conf DAG is merged with main DAG. 
  

        tango = 10
        bravo = 100
  
        task1 = {                    
          Component = TestComponent  
          Task = TestAdderTask       
          params = {                 
            num1 = 1                 
            num2 = 2                 
            result_var = tango1      
          }                          
        }                            
                                     
        task2$step1 = {
          Component: TestComponent
          Task: TestAdderTask
          dependencies = [ task1 ]
          params = {
            num1 = 10,
            num2 = 20,
              result_var = tango
          }
        }
        
        task2$step2 = {
          Component: TestComponent
          Task: TestAdderTask
          dependencies = [ task2$step1 ]
          params = {
            num1 = 30,
            num2 = ${tango}
            result_var = beta
          }
        }                           
                                     
        task3 = {                    
          Component = TestComponent  
          Task = TestAdderTask       
          dependencies = [task2$step2]     
          params = {                 
            num1 = ${bravo}         
            num2 = 2                 
            result_var = tango2      
          }                          
        }                            


  The file based worklet import mode can be used to import re-usable code. For example one could define a worklet that
  loads data from postgres database to mysql database with some processing done by a spark job. Now this worklet can 
  be imported in any number of jobs. 
  
  
#### Inline Mode:
  
  The inline mode lets you define worklets inline i.e. within the same file where you define your main job. This means
  the worklets defined in inline mode cannot be re-used in another jobs. The main reasons why in-line mode is used are
  
  * reuse the workflow multiple times within the same main DAG. If you had to repeat a complex routine multiple times
  within your main DAG multiple times inline worklet mode would help.
  
  * Another reason why inline worklets would be helpful is that any task settings defined in the DagEditor node is applied
   to all the nodes in worklet. for eg if the DagEditor node had the task setting *attempt* set to 2 then all the nodes
   imported from the worklet will have *attempt* setting set to 2. This feature is applicable for file based import as well.
   
   
  Below is an example of how inline worklet import works. all inline worklets are defined inside the **__worklet__** node.
  The **__worklet__** node has a config object where keys are the names of the worklet and the values being the dag definition
  of the worklet. This **__worklet__** config object can house multiple worklets each assigned with its unique name.
  In the below example *test_worklet* is the name of the inline worklet and the value of this key is the definition
  of the worklet.
  
  
 
        __worklet__ = {
        
          test_worklet = {
        
            deadpool = 20
        
            step1 = {
              Component: TestComponent
              Task: TestAdderTask
              params = {
                num1 = 10,
                num2 = 20,
              }
            }
        
            step2 = {
              Component: TestComponent
              Task: TestAdderTask
              dependencies = [ step1 ]
              params = {
                num1 = 30,
                num2 = ${deadpool}
              }
            }
          }
        }
        
        
        task1 = {
          Component = TestComponent
          Task = TestAdderTask
          params = {
            num1 = 1
            num2 = 2
          }
        }
        
        task2 = {
          Component = DagEditor
          Task = Import
          dependencies = [task1]
          params = {
            inline = test_worklet
          }
        }
        
        task3 = {
          Component = TestComponent
          Task = TestAdderTask
          dependencies = [task2]
          params = {
            num1 = 1
            num2 = 2
          }
        }
        
        
          
          
            
