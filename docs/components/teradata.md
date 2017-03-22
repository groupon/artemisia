
 
Teradata
========

This Component supports exporting loading and executing queries against Teradata database

| Task             | Description                                             |
|------------------|---------------------------------------------------------|
| SQLExecute       | executes DML statements such as Insert/Update/Delete    |
| SQLRead          | execute select queries and wraps the results in config  |
| SQLLoad          | load a file into a table                                |
| SQLExport        | export query results to a file                          |
| ExportToHDFS     | Export database resultset to HDFS                       |
| LoadFromHDFS     | Load Table from HDFS                                    |
| TDCHLoad         | Loads data from HDFS/Hive  into Teradata                |
| TDCHExtract      | Extract data from Teradata to HDFS/Hive                 |
| TPTLoadFromFile  | Load data from Local File-System to Teradata using TPT  |
| TPTLoadFromHDFS  | Load data to Teradata from HDFS                         |

     

 
### SQLExecute:


#### Description:

 SQLExecute task is used execute arbitary DML statements against a database

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "SQLExecute"
        params =  {
         dsn =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
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
The query must be select query and not any DML or DDL statements.
The configuration object is shown below.
    

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "SQLRead"
        params =  {
         dsn =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
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

     
           

     




### SQLLoad:


#### Description:

 
SQLLoad task is used to load content into a table typically from a file.
the configuration object for this task is as shown below.
    

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "SQLLoad"
        params =  {
         destination-table = "dummy_table @required"
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
           username = "username @required"
        }
         load =   {
           bulk-threshold = "100M @info()"
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
    * skip-lines: number of lines to skip in he table
    * quotechar: character to be used for quoting
    * bulk-threshold: size of the source file(s) above which fastload mode will be selected if auto mode is enabled
    * truncate: truncate the target table before loading data
    * error-tolerance: % of data that is allowable to get rejected value ranges from (0.00 to 1.00)
    * mode: mode of loading the table. The allowed modes are
        * fastload
        * small
        * auto
    * header: boolean field to enable/disable headers
    * escapechar: escape character used in the file
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
    
           

     




### SQLExport:


#### Description:

 
SQLExport task is used to export SQL query results to a file.
The typical task SQLExport configuration is as shown below
     

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "SQLExport"
        params =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
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
        * fastexport
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
    
           

     




### ExportToHDFS:


#### Description:

 

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "ExportToHDFS"
        params =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
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
        * fastexport
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

 

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "LoadFromHDFS"
        params =  {
         destination-table = "dummy_table @required"
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
           username = "username @required"
        }
         hdfs =   {
           cli-binary = "hdfs @default(hadoop) @info(use either hadoop or hdfs)"
           cli-mode = "yes @default(yes)"
           codec = "gzip"
           location = "/var/tmp/input.txt"
        }
         load =   {
           bulk-threshold = "100M @info()"
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
    * skip-lines: number of lines to skip in he table
    * quotechar: character to be used for quoting
    * bulk-threshold: size of the source file(s) above which fastload mode will be selected if auto mode is enabled
    * truncate: truncate the target table before loading data
    * error-tolerance: % of data that is allowable to get rejected value ranges from (0.00 to 1.00)
    * mode: mode of loading the table. The allowed modes are
        * fastload
        * small
        * auto
    * header: boolean field to enable/disable headers
    * escapechar: escape character used in the file
    * quoting: boolean field to indicate if the file is quoted.
    * delimiter: delimiter of the file
 * hdfs:
    * location: target HDFS path
    * codec: compression format to use. This field is relevant only if local-cli is false. The allowed codecs are
        * gzip
        * bzip2
        * default
    * cli-mode: boolean field to indicate if the local installed hadoop shell utility should be used to read data
    * cli-binary: hadoop binary to be used for reading. usually its either hadoop or hdfs. this field is relevant when cli-mode field is set to yes

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
    
           

     




### TDCHLoad:


#### Description:

 
 This task is used to load data to Teradata from HDFS/Hive. The hadoop task nodes directly connect to Teradata nodes (AMPs)
 and the data from hadoop is loaded to Teradata with map reduce jobs processing the data in hadoop and transferring
 them over to Teradata. Preferred method of transferring large volume of data between Hadoop and Teradata.

 This requires TDCH library to be installed on the local machine. The **source-type** can be either *hive* or *hdfs.*
 The data can loaded into Teradata in two modes.


###### batch.insert:
  Data is loaded via normal connections. No loader slots in Terdata are taken. This is ideal for loading few million
  rows of data. The major dis-advantage of this mode is that this mode has zero tolerance for rejects. so even if
  a single record is rejected the entire job fails.

###### fastload:
  Data is loaded via fastload protocol. This is ideal for loading several million records but these job occupy
  loader slots. This load is tolerant of some kind of rejects and certain rejects are persisted via the
  fastload error table such as _ET and _UV tables.

To use hive as a target the field **tdch-settings.libjars** must be set with all the

 * Hive conf dir
 * Hive library jars (jars in lib directory of hive)

 The **tdch-settings.libjars** field supports java style glob pattern. so for eg if hive lib path is located at
  `/var/path/hive/lib` and to add all the jars in the lib directory to the **tdch-settings.libjars** field one can
  use java style glob patterns such as `/var/path/hive/lib/*.jar`. so the most common value for **tdch-settings.libjars**
  will be like `libjars = ["/var/path/hive/conf", "/var/path/hive/lib/*.jar"]`.


 If you want to set any specific TDCH command line argument that is not available in this task param such as
 `targettimestampformat`, `usexviews` etc, you can use the  **tdch-settings.misc-options** field to defined these
 arguments and values. for eg the below config object would effectively result in arguments `--foo bar --hello world`
 added to the TDCH CLI command.


           misc-options = {
              foo = bar,
              hello = world
           }

    

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "TDCHLoad"
        params =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
           username = "username @required"
        }
         method = "@allowed(batch.insert, internal.fastload) @default(batch.insert)"
         source = "@required @info(hdfs path or hive table)"
         source-type = "hive @defualt(hdfs) @allowed(hive, hdfs)"
         target-table = "database.tablename @info(teradata tablename)"
         tdch-setting =   {
           format = "avrofile @default(default)"
           hadoop = "/usr/local/bin/hadoop @optional"
           hive = "/usr/local/bin/hive @optional"
           libjars = ["/path/hive/conf", "/path/hive/libs/*.jars"]
           misc-options =    {
              foo1 = "bar1"
              foo2 = "bar2"
           }
           num-mappers = "5 @default(10)"
           queue-name = "public @default(default)"
           tdch-jar = "/path/teradata-connector.jar"
           text-setting =    {
              delimiter = "| @default(,)"
              escape-char = "\\"
              quote-char = "\""
              quoting = "no @type(boolean)"
           }
        }
         truncate = "yes @default(no)"
      }
     }


#### Field Description:

 * method: defines whether to use fastload or normal jdbc insert for loading data to teradata
 * tdch-setting:
    * format: format of the file. Following are the allowed values
        * textfile
        * avrofile
        * rcfile
        * orcfile
        * sequencefile
        * parquet
    * lib-jars: list of files and directories that will be added to libjars argument and set in HADOOP_CLASSPATH environment variable.Usually the hive conf and hive lib jars are added here. The path accept java glob pattern
    * hadoop: optional path to the hadoop binary. If not specified the binary will be searched in the PATH variable
    * text-setting:
       * quote-char: character used for quoting
       * escape-char: escape character to be used. forward slash by default
       * null-string: string to represent null values
       * quoting: enable or disable quoting. both quote-char and escape-char fields are considered only when quoting is enabled
       * delimiter: delimiter of the textfile
    * hive: optional path to the hive binary. If not specified the binary will be searched in the PATH variable
    * misc-options: other TDHC arguments to be appended must be defined in this Config object
    * tdch-jar: path to tdch jar file
    * queue-name: scheduler queue where the MR job is submitted
    * num-mappers: num of mappers to be used in the MR job
 * source: hdfs path or hive tablename depending on the job-type defined
 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * truncate: truncate target table before load
 * target-table: teradata tablename
 * source-type: type of the source. currently hive and hdfs are the allowed values

#### Task Output:


     {
        taskname =  {
         __stats__ =   {
           rows = 100
        }
      }
     }

 
 **taskname.__stats__.rows__** node has the number of rows loaded by the task.
 Here it is assumed *taskname* is the name of the hypothetical export task.
    
           

     




### TDCHExtract:


#### Description:

 
 Extract data from Teradata to HDFS/Hive. The hadoop task nodes directly connect to Teradata nodes (AMPs)
 and the data from Teradata is loaded to HDFS/Hive with map reduce jobs processing the data in hadoop and extracting
 them from Teradata. This is the preferred method of transferring large volume of data between Hadoop and Teradata.

 This requires TDCH library to be installed on the local machine. The target can be either a random HDFS directory
 or a Hive table. The source can be either a Teradata table or a query. The task sports a truncate option which will
 delete the contents of target HDFS directory or truncate the data in the Hive table depending on the target-type
 selected. The **split-by** fields decides how the data is distributed and parallelized. The default value for
 this field is *hash*.

 To use hive as a target the field **tdch-settings.libjars** must be set with all the

 * Hive conf dir
 * Hive library jars (jars in lib directory of hive)

 The **tdch-settings.libjars** field supports java style glob pattern. so for eg if hive lib path is located at
  `/var/path/hive/lib` and to add all the jars in the lib directory to the **tdch-settings.libjars** field one can
  use java style glob patterns such as `/var/path/hive/lib/*.jar`. so the most common value for **tdch-settings.libjars**
  will be like `libjars = ["/var/path/hive/conf", "/var/path/hive/lib/*.jar"]`.


 If you want to set any specific TDCH command line argument that is not available in this task param such as
 `targettimestampformat`, `usexviews` etc, you can use the  **tdch-settings.misc-options** field to defined these
 arguments and values. for eg the below config object would effectively result in arguments `--foo bar --hello world`
 added to the TDCH CLI command.


           misc-options = {
              foo = bar,
              hello = world
           }

    

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "TDCHExtract"
        params =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
           username = "username @required"
        }
         source = "@required @info(tablename or sql query)"
         source-type = "@default(table) @allowed(table, query)"
         split-by = "@allowed(hash,partition,amp,value) @default(hash)"
         target = "@required @info(hdfs path or hive tablename)"
         target-type = "@default(hdfs) @allowed(hive, hdfs)"
         tdch-setting =   {
           format = "avrofile @default(default)"
           hadoop = "/usr/local/bin/hadoop @optional"
           hive = "/usr/local/bin/hive @optional"
           libjars = ["/path/hive/conf", "/path/hive/libs/*.jars"]
           misc-options =    {
              foo1 = "bar1"
              foo2 = "bar2"
           }
           num-mappers = "5 @default(10)"
           queue-name = "public @default(default)"
           tdch-jar = "/path/teradata-connector.jar"
           text-setting =    {
              delimiter = "| @default(,)"
              escape-char = "\\"
              quote-char = "\""
              quoting = "no @type(boolean)"
           }
        }
      }
     }


#### Field Description:

 * tdch-setting:
    * format: format of the file. Following are the allowed values
        * textfile
        * avrofile
        * rcfile
        * orcfile
        * sequencefile
        * parquet
    * lib-jars: list of files and directories that will be added to libjars argument and set in HADOOP_CLASSPATH environment variable.Usually the hive conf and hive lib jars are added here. The path accept java glob pattern
    * hadoop: optional path to the hadoop binary. If not specified the binary will be searched in the PATH variable
    * text-setting:
       * quote-char: character used for quoting
       * escape-char: escape character to be used. forward slash by default
       * null-string: string to represent null values
       * quoting: enable or disable quoting. both quote-char and escape-char fields are considered only when quoting is enabled
       * delimiter: delimiter of the textfile
    * hive: optional path to the hive binary. If not specified the binary will be searched in the PATH variable
    * misc-options: other TDHC arguments to be appended must be defined in this Config object
    * tdch-jar: path to tdch jar file
    * queue-name: scheduler queue where the MR job is submitted
    * num-mappers: num of mappers to be used in the MR job
 * source: defines the source table or query depending on the defined source type
 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * truncate: if target is HDFS directory it is deleted. If target is a hive table it is dropped and recreated
 * split-by: defines how the source table/query is split. allowed values being hash, partition, amp
 * target-type: defines if the target is a HDFS path or a Hive table
 * source-type: source can be either a table or a sql query. The allowed values for this field are (**table**, **query**)

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
    
           

     




### TPTLoadFromFile:


#### Description:

  Load data from a local file system to Teradata. This task is supported only in POSIX OS like Linux/Mac OS X.
  This task also expects the TPT binary installed in the local machine. It supports two mode of operations.

  * **default**: This uses TPT Stream operator to load data.
  * **fastload**: This uses TPT load operator to load data.

  To use either of the modes set **load.mode** property to *default*, *fastload* or *auto*.
  when the mode is set to *auto*, one of the two modes of *default* or *fastload* is automatically selected
  depending on the size of the data to be loaded. The property **load.bulk-threshold** defines the threshold
  for selecting the *default* and *fastload* mode. for eg if **load.bulk-threshold** is defined as 50M
  (50 Megabytes) any file(s) whose total size is lesser than 50M will be loaded by *default* mode and any file(s)
  larger than this threshold will be loaded via the *fastload* mode.

  The truncate option internally tries to delete the target table but if the target table has a fastload lock
  on the table the target table is dropped and re-created.

    

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "TPTLoadFromFile"
        params =  {
         destination-table = "target_table"
         dsn_[1] = "my_conn @info(dsn name defined in connection node)"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
           username = "username @required"
        }
         load =   {
           batch-size = "200 @default(100)"
           bulk-threshold = "100M @info()"
           delimiter = "'|' @default(',') @type(char)"
           error-file = "/var/path/error.txt @optional"
           error-limit = "1000 @default(2000)"
           error-tolerence = "0.57 @default(2) @type(double,0,1)"
           escapechar = "\" @default(\\) @type(char)"
           header = "no @default(false) @type(boolean)"
           mode = "default @default(default) @type(string)"
           null-string = "\\N @optional @info(marker string for null)"
           quotechar = "\" @default('\"') @type(char)"
           quoting = "no @default(false) @type(boolean)"
           skip-lines = "0 @default(0) @type(int)"
           truncate = "yes @type(boolean)"
        }
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * destination-table: destination table to load
 * location: path pointing to the source file
 * load:
    * skip-lines: number of lines to skip in he table
    * quotechar: character to be used for quoting
    * bulk-threshold: size of the source file(s) above which fastload mode will be selected if auto mode is enabled
    * dtconn-attrs: miscellaneous data-connector operator attributes
    * error-file: location of the reject file
    * truncate: truncate the target table before loading data
    * error-limit: maximum number of records allowed in error table
    * error-tolerance: % of data that is allowable to get rejected value ranges from (0.00 to 1.00)
    * mode: mode of loading the table
    * header: boolean field to enable/disable headers
    * escapechar: escape character used in the file
    * null-string: marker string for null. default value is blank string
    * quoting: boolean field to indicate if the file is quoted.
    * delimiter: delimiter of the file
    * load-attrs: miscellaneous load operator attributes

#### Task Output:


 The output config can be two types depending on the mode of operation.
 
 **default**:
 
   The output of the default mode is shown below.
 
        taskname = {
           __stats__ = {
              loaded = 90
              error-file = 5
              error-table = 5
              rejected = 10
              source = 100
           }
         }
 
   * *loaded*: number of records inserted
   * *error-file*: number of records sent to error file.
   * *error-table*: number of records sent to error table.
   * *rejected*: total number of records rejected. This is a derived field with formula error-table + error-file.
   * *source*: total number of records in source. This is a derived field with formula loaded + error-table + error-file.
 
 
 **fastload**:
 
  The output of the fastload mode is
 
       taskname = {
         __stats__ = {
             sent = 90
             loaded = 78
             err_table1 = 5
             err_table2 = 5
             duplicate = 2
             err_file = 10
             source = 100
             rejected = 22
         }
       }
 
   * *sent*: number of records sent to the database. i.e. (source - err_file)
   * *loaded*: number of records inserted in the target table
   * *err_table1*: number of records sent to the error table 1
   * *err_table2*: number of records sent to the error table 2
   * *duplicate*: number of records ignored for being duplicates
   * *err_file*: number of records sent to error-file
   * *source*: number of rows in the source file(s). This is a derived field with formula (sent + err_file)
   * *rejected*: number of records rejected. This is a derived field with formula (err_table1 + err_table2 + duplicate + err_file)
 
     

     




### TPTLoadFromHDFS:


#### Description:

 
 Load data from a HDFS filesystem to Teradata. This task is supported only in POSIX OS like Linux/Mac OS X.
  This task also expects the TPT binary installed in the local machine. It supports two mode of operations.

  * **default**: This uses TPT Stream operator to load data.
  * **fastload**: This uses TPT load operator to load data.

  To use either of the modes set **load.mode** property to *default*, *fastload* or *auto*.
  when the mode is set to *auto*, one of the two modes of *default* or *fastload* is automatically selected
  depending on the size of the data to be loaded. The property **load.bulk-threshold** defines the threshold
  for selecting the *default* and *fastload* mode. for eg if **load.bulk-threshold** is defined as 50M
  (50 Megabytes) any file(s) whose total size is lesser than 50M will be loaded by *default* mode and any file(s)
  larger than this threshold will be loaded via the *fastload* mode.

  The truncate option internally tries to delete the target table but if the target table has a fastload lock
  on the table the target table is dropped and re-created.
    

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "TPTLoadFromHDFS"
        params =  {
         destination-table = "target_table"
         dsn_[1] = "my_conn @info(dsn name defined in connection node)"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
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
           bulk-threshold = "100M @info()"
           delimiter = "'|' @default(',') @type(char)"
           error-file = "/var/path/error.txt @optional"
           error-limit = "1000 @default(2000)"
           error-tolerence = "0.57 @default(2) @type(double,0,1)"
           escapechar = "\" @default(\\) @type(char)"
           header = "no @default(false) @type(boolean)"
           mode = "default @default(default) @type(string)"
           null-string = "\\N @optional @info(marker string for null)"
           quotechar = "\" @default('\"') @type(char)"
           quoting = "no @default(false) @type(boolean)"
           skip-lines = "0 @default(0) @type(int)"
           truncate = "yes @type(boolean)"
        }
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * destination-table: destination table to load
 * load:
    * skip-lines: number of lines to skip in he table
    * quotechar: character to be used for quoting
    * bulk-threshold: size of the source file(s) above which fastload mode will be selected if auto mode is enabled
    * dtconn-attrs: miscellaneous data-connector operator attributes
    * error-file: location of the reject file
    * truncate: truncate the target table before loading data
    * error-limit: maximum number of records allowed in error table
    * error-tolerance: % of data that is allowable to get rejected value ranges from (0.00 to 1.00)
    * mode: mode of loading the table
    * header: boolean field to enable/disable headers
    * escapechar: escape character used in the file
    * null-string: marker string for null. default value is blank string
    * quoting: boolean field to indicate if the file is quoted.
    * delimiter: delimiter of the file
    * load-attrs: miscellaneous load operator attributes
 * hdfs:
    * location: target HDFS path
    * codec: compression format to use. This field is relevant only if local-cli is false. The allowed codecs are
        * gzip
        * bzip2
        * default
    * cli-mode: boolean field to indicate if the local installed hadoop shell utility should be used to read data
    * cli-binary: hadoop binary to be used for reading. usually its either hadoop or hdfs. this field is relevant when cli-mode field is set to yes

#### Task Output:


 The output config can be two types depending on the mode of operation.
 
 **default**:
 
   The output of the default mode is shown below.
 
        taskname = {
           __stats__ = {
              loaded = 90
              error-file = 5
              error-table = 5
              rejected = 10
              source = 100
           }
         }
 
   * *loaded*: number of records inserted
   * *error-file*: number of records sent to error file.
   * *error-table*: number of records sent to error table.
   * *rejected*: total number of records rejected. This is a derived field with formula error-table + error-file.
   * *source*: total number of records in source. This is a derived field with formula loaded + error-table + error-file.
 
 
 **fastload**:
 
  The output of the fastload mode is
 
       taskname = {
         __stats__ = {
             sent = 90
             loaded = 78
             err_table1 = 5
             err_table2 = 5
             duplicate = 2
             err_file = 10
             source = 100
             rejected = 22
         }
       }
 
   * *sent*: number of records sent to the database. i.e. (source - err_file)
   * *loaded*: number of records inserted in the target table
   * *err_table1*: number of records sent to the error table 1
   * *err_table2*: number of records sent to the error table 2
   * *duplicate*: number of records ignored for being duplicates
   * *err_file*: number of records sent to error-file
   * *source*: number of rows in the source file(s). This is a derived field with formula (sent + err_file)
   * *rejected*: number of records rejected. This is a derived field with formula (err_table1 + err_table2 + duplicate + err_file)
 
     

     

     