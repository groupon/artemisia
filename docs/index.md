# Artemisia

Artemisia is a free open-source light-weight configuration driven Data-Integration utility.
In Artemisia all jobs are defined in [HOCON](https://github.com/typesafehub/config/blob/master/HOCON.md) configuration format.
It currently supports *Components* such as MySQL, Postgres, Teradata, Hive, Hadoop etc
The following characteristic feature of Artemisia makes it more simpler and easier tool to learn and work with than most
Data Integration tools.
  
 * Simple
 * Configuration driven
 * Modular
 * Importable
 * Bare-bone


##### Simple:

  ETL Systems were designed during a time where MPP (Massively Parallel Processing) systems were uncommon or were 
prohibitively expensive. So ETL Systems did most of the processing (cleaning and transforming) of data.
ETL Systems introduced their own DSL and components which had to be learned by the developers in addition to the Data Systems
such as RDBMS and its associated technologies like SQL/Stored Procedures etc. All this was done because for the most part neither the source
nor the target systems (like a RDBMS or NoSQL systems) were scalable and weren't capable of handling both the ETL, reporting
and other operational tasks.

  Fast forward now we have the hadoop eco-system and most of the OLAP based RDBMS are distributed and scalable. MPP based 
systems are far more prevalent now in the parlance of our time. This means that all or atleast most of data processing needs
can now be off-loaded to these MPP systems like Hadoop, Spark or any distributed relational database. 

  
  Thus new ETL tools doesn't have to be bloated with complex data processing capabilities but instead it should integrate
with existing MPP and non-MPP systems and leverage their's data processing capabilities. This design principle makes Artemisia
extremely lightweight and simple to learn and master.


##### Configuration Driven:

  Artemisia is fully configuration driven. The entire job definition is defined in a Hocon configuration file. This 
  [link](https://github.com/typesafehub/config/blob/master/HOCON.md) will have more information about Hocon and its 
  implementation typesafe config. All the job settings, variables and actual job steps are all defined as a Hocon
  configuration and no programming skills are required to create a job. The configuration driven
  nature of Artemisia makes it 
    
  * very easy for developers to learn and start using it
  * makes writing ETL jobs easy
  * makes reading ETL even easier
  * custom applications can generate job definition on the fly to create dynamic ETLs.
  * simplified version control. the config files are the only files to be managed by the version control of your choice.
  
  
##### Modular:
  
  Artemisia is not a single monolithic product. The application is composed of the core Artemisia module and a module
  for each Components such as MySQL, Postgres, Hadoop, Teradata etc. They are separate projects that produces their own
  jar artifacts and are independently compilable and testable.  
    
    
##### Importable:
   
  Your Java or any JVM based applications can declare a dependency on a Artemisia module and leverage the tasks supported
  by the said module. For eg: your application can declare dependency on MySQL module and use it load data from a file 
  to MySQL table or export data from a Mysql table to a file. This allows for deeper integration of Artemisia's modules
  with your JVM based applications written on (Scala/Java/Clojure etc).
  

##### Bare-bone:
 
 As of now Artemisia is command line utility that requires very little dependency and setup. As opposed to most other
 ETL Systems it requires no backend databases or special clusters to operate. It also doesnt comes with its own scheduler as of now
 and hence can be used with any scheduler.

 