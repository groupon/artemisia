
 
Postgres
========

Component for interacting with postgres database

| Task          | Description                                             |
|---------------|---------------------------------------------------------|
| SQLExport     | export query results to a file                          |
| SQLLoad       | load a file into a table                                |
| SQLExecute    | executes DML statements such as Insert/Update/Delete    |
| SQLRead       | execute select queries and wraps the results in config  |
| ExportToHDFS  | Export database resultset to HDFS                       |
| LoadFromHDFS  | Load Table from HDFS                                    |

     

 
### SQLExport:


#### Description:

 
SQLExport task is used to export SQL query results to a file.
The typical task SQLExport configuration is as shown below
     

#### Configuration Structure:


      {
        Component = "Postgres"
        Task = "SQLExport"
        params =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "5432 @default(5432)"
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
        * bulk
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
    
           

     




### SQLLoad:


#### Description:

 
SQLLoad task is used to load content into a table typically from a file.
the configuration object for this task is as shown below.
    

#### Configuration Structure:


      {
        Component = "Postgres"
        Task = "SQLLoad"
        params =  {
         destination-table = "dummy_table @required"
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "5432 @default(5432)"
           username = "username @required"
        }
         load =   {
           batch-size = "200 @default(100)"
           delimiter = "'|' @default(',') @type(char)"
           error-tolerence = "0.57 @default(2) @type(double,0,1)"
           escapechar = "\" @default(\\) @type(char)"
           header = "no @default(false) @type(boolean)"
           mode = "default @default(default) @type(string)"
           quotechar = "\" @default('\"') @type(char)"
           quoting = "no @default(false) @type(boolean)"
           skip-lines = "0 @default(0) @type(int)"
           truncate = "yes @type(boolean)"
        }
         location = "/var/tmp/file.txt"
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * destination-table: destination table to load
 * location: path pointing to the source file
 * load:
    * skip-lines: number of lines to skip
    * quotechar: character to be used for quoting
    * truncate: truncate the target table before loading data
    * error-tolerance: % of data that is allowable to get rejected value ranges from (0.00 to 1.00)
    * mode: mode of loading the table
    * header: boolean field to enable/disable headers
    * escapechar: escape character used in the file
    * batch-size: loads into table will be grouped into batches of this size.
    * quoting: boolean field to indicate if the file is quoted.
    * delimiter: delimiter of the file

#### Task Output:


     {
        taskname =  {
         __stats__ =   {
           loaded = 100
           rejected = 2
        }
      }
     }

 
 **taskname.__stats__.loaded** and **taskname.__stats__.rejected** keys have the numbers of records
 loaded and the number of records rejected respectively.
    
           

     




### SQLExecute:


#### Description:

 SQLExecute task is used execute arbitrary DML/DDL statements against a database

#### Configuration Structure:


      {
        Component = "Postgres"
        Task = "SQLExecute"
        params =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "5432 @default(5432)"
           username = "username @required"
        }
         sql = "DELETE FROM TABLENAME @optional(either this or sqlfile key is required)"
         sqlfile = "/var/tmp/sqlfile.sql @optional(either this or sql key is required)"
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * sql: select query to be run
 * sqlfile: the file containing the query

#### Task Output:


     {
        taskname =  {
         __stats__ =   {
           rows-effected = 52
        }
      }
     }

 
 **taskname.__stats__.row-effected** key has the total number of row effected (inserted/deleted/updated)
 by the sql query.
    
           

     




### SQLRead:


#### Description:

 
SQLRead task runs a select query and parse the first row as a Hocon Config.
The query must be a SELECT query and not any DML or DDL statements.
The configuration object is shown below.
    

#### Configuration Structure:


      {
        Component = "Postgres"
        Task = "SQLRead"
        params =  {
         dsn =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "5432 @default(5432)"
           username = "username @required"
        }
         sql = "SELECT count(*) as cnt from table @optional(either this or sqlfile key is required)"
         sqlfile = "/var/tmp/sqlfile.sql @optional(either this or sql key is required)"
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
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

     
           

     




### ExportToHDFS:


#### Description:

 ExportToHDFS is used to export SQL query results to a HDFS file

#### Configuration Structure:


      {
        Component = "Postgres"
        Task = "ExportToHDFS"
        params =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "5432 @default(5432)"
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
         hdfs =   {
           block-size = "120M"
           codec = "gzip"
           location = "/user/hadoop/test"
           overwrite = "no"
           replication = "2 @default(3) @info(allowed values 1 to 5)"
        }
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
        * bulk
    * header: boolean literal to enable/disable header
    * sqlfile: used in place of sql key to pass the file containing the SQL
    * escapechar: escape character use for instance to escape delimiter values in field
    * quoting: boolean literal to enable/disable quoting of fields.
    * delimiter: character to be used for delimiter
 * hdfs:
    * location: target HDFS path
    * replication: replication factor for the file. only values 1 to 5 are allowed
    * block-size: HDFS block size of the file
    * codec: compression format to use. The allowed codecs are
        * gzip
        * bzip2
        * default
    * overwrite: overwrite target file it already exists

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
    
           

     




### LoadFromHDFS:


#### Description:

 LoadFromHDFS can be used to load file(s) from a HDFS path

#### Configuration Structure:


      {
        Component = "Postgres"
        Task = "LoadFromHDFS"
        params =  {
         destination-table = "dummy_table @required"
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "3306 @default(3306)"
           username = "username @required"
        }
         hdfs =   {
           cli-binary = "hdfs @default(hadoop) @info(use either hadoop or hdfs)"
           cli-mode = "yes @default(yes)"
           codec = "gzip"
           location = "/var/tmp/input.txt"
        }
         load =   {
           batch-size = "200 @default(100)"
           delimiter = "'|' @default(',') @type(char)"
           error-tolerence = "0.57 @default(2) @type(double,0,1)"
           escapechar = "\" @default(\\) @type(char)"
           header = "no @default(false) @type(boolean)"
           mode = "default @default(default) @type(string)"
           quotechar = "\" @default('\"') @type(char)"
           quoting = "no @default(false) @type(boolean)"
           skip-lines = "0 @default(0) @type(int)"
           truncate = "yes @type(boolean)"
        }
         location = "/var/tmp/file.txt"
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * destination-table: destination table to load
 * load:
    * skip-lines: number of lines to skip
    * quotechar: character to be used for quoting
    * truncate: truncate the target table before loading data
    * error-tolerance: % of data that is allowable to get rejected value ranges from (0.00 to 1.00)
    * mode: mode of loading the table
    * header: boolean field to enable/disable headers
    * escapechar: escape character used in the file
    * batch-size: loads into table will be grouped into batches of this size.
    * quoting: boolean field to indicate if the file is quoted.
    * delimiter: delimiter of the file
 * hdfs:
    * location: target HDFS path
    * codec: compression format to use. This field is relevant only if local-cli is false. The allowed codecs are
        * gzip
        * bzip2
        * default
    * cli-mode: boolean field to indicate if the local installed hadoop shell utility should be used to read data
    * cli-binary: hadoop binary to be used for reading. usually it's either hadoop or HDFS. this field is relevant when cli-mode field is set to yes

#### Task Output:


     {
        taskname =  {
         __stats__ =   {
           loaded = 100
           rejected = 2
        }
      }
     }

 
 **taskname.__stats__.loaded** and **taskname.__stats__.rejected** keys have the numbers of records
 loaded and the number of records rejected respectively.
    
           

     

     