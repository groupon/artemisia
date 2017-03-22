
MySQL
=======

This is a sample job that uses mysql components

* step `sqlread` executed a select query and creates a new variable called `row_cnt` assigned with the query output.
         
* step `sqlexport` a table to a file `output.txt` with headers. It also asserts if atleast 1 records were exported.
        
* step `truncate` truncates the target table `profile_test`

* step `load_from_file` loads the exported file to the target table `profile_test`.        
        
        src_table = my_src_table
        tgt_table = my_tgt_table

        sqlread = {
            Component = MySQL
            Task = SQLRead
            params = {
              dsn = myconn
              sql = "select count(*) as row_cnt from ${src_table}"
            }
        }

        sqlexport = {
          Component = MySQL
          Task = SQLExport
          dependencies = [sqlread]
          params = {
            dsn = myconn
            export = {
              mode = "default"
              header = yes
            }
            sql = "select * from ${src_table} limit ${row_cnt}"
            location = output.txt
          }
          assert = "${?sqlexport.__stats__.rows} > 1" // assert if more than one row is exported by the task
        }

        truncate = {
          Component = MySQL
          Task = SQLExecute
          dependencies = [sqlexport]
            params = {
              dsn = myconn
              sql = delete from ${tgt_table}
            }
        }

        load_from_file = {
           Component = MySQL
           Task = SQLLoad
           dependencies = [truncate]
           ignore-error = yes
             params = {
               dsn = myconn
               destination-table = ${tgt_table}
               location = output.txt
               load-setting = {
                 mode = "bulk"
                 header =  yes
                 delimiter = ","
                 quoting = no,
               }
            }
            assert = "${load_from_file.__stats__.loaded} == ${sqlexport.__stats__.rows}"
         }


         __connections__ = {
          myconn = {
             host = mysql_server_name
             username = db_username
             password = db_password
             database = db_schema
             port = 3306
          }
        }
        
        
        
        
