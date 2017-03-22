
Teradata
=======

This is a sample job that uses Teradata components

* step `sqlread` executed a select query and creates a new variable called `row_cnt` assigned with the query output.

* step `sqlexport` a table to a file `output.txt` with headers. It also asserts if atleast 1 records were exported.

* step `truncate` truncates the target table `profile_test`

* step `load_from_file` loads the exported file to the target table `profile_test`.


        src_table = db.my_src_table
        tgt_table = db.my_tgt_table

        sqlread = {
            Component = Teradata
            Task = SQLRead
            params = {
              dsn = myconn
              sql = "select count(*) as row_cnt from ${src_table}"
            }
        }

        sqlexport = {
          Component = Teradata
          Task = SQLExport
          dependencies = [sqlread]
          params = {
            dsn = myconn
            export = {
              mode = "default"
              header = yes
            }
            sql = "select * from ${src_table} sample ${row_cnt}"
            location = output.txt
          }
          assert = "${?sqlexport.__stats__.rows} > 1" // assert if more than one row is exported by the task
        }

        truncate = {
          Component = Teradata
          Task = SQLExecute
          dependencies = [sqlexport]
            params = {
              dsn = myconn
              sql = delete from ${tgt_table}
            }
        }

        load_from_file = {
           Component = Teradata
           Task = TPTLoadFromFile
           dependencies = [truncate]
           ignore-error = yes
             params = {
               dsn = myconn
               destination-table = ${tgt_table}
               location = output.txt
               load = {
                 mode = fastload
                 header =  yes
                 delimiter = ","
                 quoting = no,
               }
            }
            when = "1 == 0"
            assert = "${load_from_file.__stats__.loaded} == ${sqlexport.__stats__.rows}"
         }


         __connections__ = {
          myconn = {
             host = td_host
             username = td_username
             password = td_password
             database = td_db
             port = 1025
          }
        }