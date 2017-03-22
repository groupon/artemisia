
 
Hive
====

A component for hive interaction

| Task        | Description                                             |
|-------------|---------------------------------------------------------|
| HQLExecute  | Execute Hive HQL queries                                |
| HQLExport   | export query results to a file                          |
| HQLRead     | execute select queries and wraps the results in config  |

     

 
### HQLExecute:


#### Description:

 
 HQLExecute is used to execute Hive DML/DDL queries (INSERT/CREATE etc). This task can take a HiveServer2 connection
 as input param and execute the query by connecting to the HiveServer2 service. If no connection is provided
 it would use local Hive CLI to execute the query.
    

#### Configuration Structure:


      {
        Component = "Hive"
        Task = "HQLExecute"
        params =  {
         dsn_[1] = "connection-name @optional"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "10000 @default(10000)"
           username = "username @required"
        }
         sql = "DELETE FROM TABLENAME @optional(either this or sqlfile key is required)"
         sqlfile = "/var/tmp/sqlfile.sql @optional(either this or sql key is required)"
      }
     }


#### Field Description:

 * dsn:
 either a name of the dsn or a config-object with username/password and other credentials.
 This field is optional field and if not provided then task would use the local Hive CLI installation to execute the query
     
 * sql: select query to be run
 * sqlfile: the file containing the query

#### Task Output:


     {
        taskname =  {
         __stats__ =   {
           loaded =    {
              tablename_1 = 52
              tablename_2 = 100
           }
        }
      }
     }

 
 Here the hypothetical task has two insert statements that updates two tables *tablename_1* and *tablename_2*.
 *tablename_1* has modified 52 rows and *tablename_2* has modified 100 rows.
    
           

     




### HQLExport:


#### Description:

 
HQLExport task is used to export SQL query results to a file.
The typical task HQLExport configuration is as shown below.
Unlike HQLExecute this task requires a HiveServer2 connection and cannot leverage local CLI installation.
    

#### Configuration Structure:


      {
        Component = "Hive"
        Task = "HQLExport"
        params =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "10000 @default(10000)"
           username = "username @required"
        }
         export =   {
           delimiter = "| @default(,) @type(char)"
           escapechar = "'\\' @default(\\) @type(char)"
           header = "yes @default(false) @type(boolean)"
           mode = "default @default(default)"
           quotechar = "'\"' @default(\") @type(char)"
           quoting = "yes @default(false) @type(boolean)"
        }
         location = "/var/tmp/file.txt"
         sql = "SELECT * FROM TABLE @optional(either sql or sqlfile key is required)"
         sql-file = "run_queries.sql @info(path to the file) @optional(either sql or sqlfile key is required)"
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * export:
    * quotechar: quotechar to use if quoting is enabled.
    * mode: modes of export. supported modes are
        * default
    * header: boolean literal to enable/disable header
    * sqlfile: used in place of sql key to pass the file containing the SQL
    * escapechar: escape character use for instance to escape delimiter values in field
    * quoting: boolean literal to enable/disable quoting of fields.
    * delimiter: character to be used for delimiter
 * location: path to the target file

#### Task Output:


     {
        taskname =  {
         __stats__ =   {
           rows = 100
        }
      }
     }

 
 **taskname.__stats__.rows__** node has the number of rows exported by the task.
 Here it is assumed *taskname* is the name of the hypothetical export task.
    
           

     




### HQLRead:


#### Description:

 
The HQLRead task lets you a run a SELECT query which returns a single row. This single row is
processed back and converted to a JSON/HOCON map object and merged with job context so that values
are available in the downstream task.
    

#### Configuration Structure:


      {
        Component = "Hive"
        Task = "HQLRead"
        params =  {
         dsn =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "10000 @default(10000)"
           username = "username @required"
        }
         sql = "SELECT count(*) as cnt from table @optional(either this or sqlfile key is required)"
         sqlfile = "/var/tmp/sqlfile.sql @optional(either this or sql key is required)"
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials.
 This field is optional field and if not provided then task would use the local Hive CLI installation to execute the query
 
 * sql: select query to be run
 * sqlfile: the file containing the query

#### Task Output:


     {
        cnt = 100
        foo = "bar"
     }

 
 The first row of the result is retrieved and parsed to generate a config object.
 The column-names are turned as keys and the values of the first row of the result-set
 are turned into corresponding values. If there are more records in the result-set only
 the first record is considered and the rest are ignored. As for the above config example
 the input query would look like this.

     SELECT count(*) as cnt, 'bar' as foo FROM database.tablename

     
           

     

     