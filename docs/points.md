
* you cannot have nodes with name that starts and end with double underscore __. for eg `__step1__` is not valid task_name.
  this task will be ignored when the config is parsed and Dag nodes are generated.

* for configuration nodes that expects character data types like delimiter. you have a single character or multiline encoded characters 