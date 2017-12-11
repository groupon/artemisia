# Installation
  
  The application can be distributed as a zip archive. The installation is as simple as downloading the application and
  edit the PATH variable to include  artemisia *bin*. You can download the built binary version from the Github repository
  or you can choose to build your own version if you require any customization especially with versioning of dependencies.
  Follow the below steps to build artemisia by yourself.
  
# Building the application

  you can build the application by yourself by following the below steps after cloning the repository.

  * navigate to *project/Dependencies.scala* and make the following changes
    * ensure `packageMode` value is set to `true`
    * if required change `HADOOP_VERSION` value to the version suitable to your environment.
    * if required change `HIVE_SERVER_VERSION` value to the version suitable to your environment.
    * change the version of other dependencies if required (not recommended unless necassary)

  * run `reload` command in your sbt console or open a new sbt console session.

  * run `stage` command

  * when the `stage` command execution completes your compiled binary artemisia package should be ready in a directory
    called *stage* in path *target/universal*

  * navigate to file *target/universal/stage/conf/settings.conf* and change any default value for any tasks or any application
    settings if required.

  * the `Teradata` component requires jdbc drivers to work. Due to licensing reasons the Teradata binaries are not included
    in the package. so you must download them [here](https://downloads.teradata.com/download/connectivity/jdbc-driver).
    Place the downloaded jdbc driver jar files in *target/universal/stage/lib* path. you can skip this step if you don't
    need Teradata support for your application. Please note that any jar files dropped in the lib folder will automatically
    added to the classpath of the application.

  * rename *stage* to *artemisia* and copy the package/directory to any location and ensure your `PATH` environment
    variable include *<deployment_path>/artemisia/bin*

# Verify Installation

  Once the installation is complete you can verify the installation by running the command `artemisia -h`. And you should
  see the below output in your console.


      $ artemisia -h
      Usage:  [options]

        -h | -help         print this message
        -v | -verbose      this runner is chattier
        -d | -debug        set sbt log level to debug
        -no-version-check  Don't run the java version check.
        -main <classname>  Define a custom main class
        -jvm-debug <port>  Turn on JVM debugging, open at the given port.

        # java version (default: java from PATH, currently java version "1.7.0_21")
        -java-home <path>         alternate JAVA_HOME

        # jvm options and output control
        JAVA_OPTS          environment variable, if unset uses ""
        -Dkey=val          pass -Dkey=val directly to the java runtime
        -J-X               pass option -X directly to the java runtime
                           (-J is stripped)

        # special option
        --                 To stop parsing built-in commands from the rest of the command-line.
                           e.g.) enabling debug and sending -d as app argument
                           $ ./start-script -d -- -d

      In the case of duplicated or conflicting options, basically the order above
      shows precedence: JAVA_OPTS lowest, command line options highest except "--".
