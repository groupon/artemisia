
 
Core
====

Component that supports core tasks of Artemisia

| Task         | Description                                                                                 |
|--------------|---------------------------------------------------------------------------------------------|
| ScriptTask   | executes script with customizable interpreter                                               |
| EmailTask    | EmailTask is used to send Email notifications.                                              |
| SFTPTask     | SFTPTask supports copying files from remote sftp server to local filesystem and vice versa  |
| RestAPITask  | execute HTTP calls and handle results                                                       |

     

 
### ScriptTask:


#### Description:

 
ScriptTask is used to execute scripts in a shell native to the operating system;
For example *Bash* in *Linux*. The content of the `script` node is flushed into a temporary script file
and the script file is executed by the shell interpreter specified in `interpreter` node.
    

#### Configuration Structure:


      {
        Component = "Core"
        Task = "ScriptTask"
        params =  {
         params =   {
           cwd = "/var/tmp @default(<your current working directory>)"
           env = "{ foo = bar, hello = world } @default(<empty object>)"
           interpreter = "/usr/local/bin/sh @default(/bin/sh)"
           parse-output = "yes @default(false)"
           script = "echo Hello World @required"
        }
      }
     }


#### Field Description:

 * parse-output: parse the stdout of script which has to be a Hocon config (Json superset) and merge the result to the job config
 * cwd: set the current working directory for the script execution
 * script: string whose content while be flushed to a temp file and executed with the interpreter
 * interpreter: the interpreter used to execute the script. it can be bash, python, perl etc
 * env: environmental variables to be used

#### Task Output:


     {
        foo = "bar"
     }

 
 This task has two modes depending on parse-output flag being set to true or not.

  * when it is set to true the output of the script is parsed as Hocon config. In the above example we emit a
  object with key foo and value bar.

  * when it is set to false the task emits an empty config object.
    
           

     




### EmailTask:


#### Description:

 EmailTask is used to send Email notifications.

#### Configuration Structure:


      {
        Component = "Core"
        Task = "EmailTask"
        params =  {
         params =   {
           connection_[0] = "email_connection"
           connection_[1] =    {
              from = "xyz@example.com"
              host = "host @required"
              password = "password"
              port = "-1 @required"
              reply-to = "xyx@example.com"
              ssl = "no @default(no) @type(boolean)"
              tls = "no @default(no) @type(boolean)"
              username = "username"
           }
           email =    {
              attachment_[0] = "['/var/tmp/file1.txt', '/var/tmp/file2.txt'] @optional"
              attachment_[1] = "[{'attachment1.txt', '/var/tmp/file1.txt'}, {'attachment2.txt', '/var/tmp/file2.txt'}] @optional"
              bcc_[0] = "xyz@example.com @optional"
              bcc_[1] = "[ xyz1@example.com, xyz2@example.com ] @optional"
              cc_[0] = "xyz@example.com @optional"
              cc_[1] = "[ xyz1@example.com, xyz2@example.com ] @optional"
              message = "message"
              subject = "subject"
              to_[0] = "xyz@example.com"
              to_[1] = ["xyz1@example.com", "xyz2@example.com"]
           }
        }
      }
     }


#### Field Description:

 * connection:
    * username: username used for authentication
    * host: SMTP host address
    * ssl: boolean field enabling ssl
    * reply-to: replies to the sent email will be addressed to this address
    * tls: boolean field for enabling tls
    * port: port of the stmp server
    * from: from address to be used
    * password: password used for authentication
 * email:
    * to: to address list. it can either be a single email address string or an array of email address
    * cc: cc address list. same as to address both string and array is supported
    * bcc: bcc address list. same as to address both string and array is supported
    * attachment: 
 can be a array of strings or objects. If string it must be a path to the file. if Object it must have one key
 and value. The key will be name of the attached file and value will be the path of the file. In the  example
 `[{'attachment1.txt', '/var/tmp/file1.txt'}, {'attachment2.txt', '/var/tmp/file2.txt'}]`. There are two files in the attachment.
 attachment 1 with name `attachment1.txt` is a file loaded from the path /var/tmp/file1.txt.
 And similarly attachment 2 is a file with name `attachment2.txt` loaded from the path /var/tmp/file2.txt.

#### Task Output:

This task outputs a empty config object

     




### SFTPTask:


#### Description:

 
SFTPTask is used to perform `put` and `get` operations of SFTP.

**PUT:**

 The PUT operation will move your file from local system to the SFTP server. The target path can be either the current
 working directory of SFTP session (which can be set using the setting `remote-dir`) or a user provided path.
 for example in the below setting we are uploading two local files to the SFTP server. The first file `/var/tmp/file1.txt`
 is uploaded to the SFTP path `/sftp_root_dir/path/file1.txt`. The second local file `/var/tmp/file2.txt` is uploaded
 to the path `/sftp_root_dir/dir1/files/file2.txt` because the current working directory of the SFTP shell is
 `/sftp_root_dir/dir1/files` as set by the `remote_dir`.

      put = [{ "/var/tmp/file1.txt" = /sftp_root_dir/path/file1.txt }, "/var/tmp/file2.txt" ]
      remote_dir = /sftp_root_dir/dir1/files

**GET**:

 The GET operation will move files from your SFTP server to your local server. The target path can be either the current
 local working directory (configurable via `local-dir` setting) or the path provided by the user. In the below
 example we move two files `/root_sftp_dir/file1.txt` to local path `/var/tmp/file1.txt` and `/root_sftp_dir/file2.txt`
 to `/var/tmp/file2.txt`

      get = [{ '/root_sftp_dir/file1.txt' = '/var/tmp/file1.txt' },'/root_sftp_dir/file2.txt']
      local-dir = /var/tmp

     

#### Configuration Structure:


      {
        Component = "Core"
        Task = "SFTPTask"
        params =  {
         params =   {
           connection_[0] = "sftp_connection_name"
           connection_[1] =    {
              hostname = "sftp-host-name @required"
              password = "sftppassword @optional(not required if key based authentication is used)"
              pkey = "/home/user/.ssh/id_rsa @optional(not required if username/password authentication is used)"
              port = "sftp-host-port @default(22)"
              username = "sftp-username @required"
           }
           get = "[{ '/root_sftp_dir/file1.txt' = '/var/tmp/file1.txt' },'/root_sftp_dir/file2.txt'] @type(array)"
           local-dir = "/var/tmp @default(your current working directory.) @info(current working directory)"
           put = "[{ '/var/tmp/file1.txt' = '/sftp_root_dir/file1.txt' },'/var/tmp/file1.txt'] @type(array)"
           remote-dir = "/root @info(remote working directory)"
        }
      }
     }


#### Field Description:

 * local-dir: set local working directory. by default it will be your current working directory
 * remote-dir: set remote working directory
 * put: array of object or strings providing source and target (optional if type is string) paths
 * get: array of object or strings providing source and target (optional if type is string) paths
 * connection:
    * hostname: hostname of the sftp-server
    * username: username to be used for sftp connection
    * pkey: optional private key to be used for the connection
    * port: sftp port number
    * password: optional password for sftp connection if exists

#### Task Output:

This task outputs a empty config object

     




### RestAPITask:


#### Description:

 
 The Rest API task can be used to make HTTP calls. The following HTTP methods are supported
 (get,head,put,post,patch,delete).
 The HTTP methods *get*, *head* cannot have request body while other methods can have request body. The payload can either
 be of type *json*, *xml*, *text*. if the payload is of type *json* the body field type can be a Hocon type ie
 Hocon Config Object type or Hocon Config Array since Hocon is a superset of Json. for eg the below setting is prefectly legal
 and allowed. In the below case the *body* field type is a Hocon Config Object. It can also be of Hocon Config Array type.

     request = {
       url = http://api.example.com
       body = {
         hello = world
         content = [foo, bar, baz]
       }
     }

 If your body is of type *xml* or *text* then the *body* field type has to be of type string. for example the below
 example shows how to post a xml content.

     request = {
       url = http://api.example.com/post
       method = post
       body = "<foo><bar>baz</bar></foo>"
     }

 *emit-output* field takes effect only if the HTTP response body is of type JSON. It parses the Json response body
 and converts it to a Hocon config object and returns it as the output of the task. *emit-output* will also work
 only when the JSON response is of type object and not an array. This is because the output of each task should be
 of type ConfigObject and cannot be ConfigArray. for eg if the response of an API call was as shown below
 and the *emit-output* field was set to true. Then the json response is parse and converted to Hocon config object
 and it will be merged back to the job config. So the downstream tasks can refer to variable foo (${foo}) and the value
 will be resolved to "bar" successfully.

       {
         "foo" : "bar"
       }

 This will not work if the response of the Rest API call was an json array like `["foo", "bar"]` due to above mentioned reasons.

 *allowed-status-codes* field takes a array of integer which represents the list of valid HTTP status code that an
 HTTP call can have. for example if *allowed-status-codes* is set to `[200]` and if a Rest API call returns 404
 then the API call is assumed to have failed and hence the task execution is failed.


    

#### Configuration Structure:


      {
        Component = "Core"
        Task = "RestAPITask"
        params =  {
         allowed-status-codes = [200, 100]
         emit-output = "yes"
         request =   {
           body =    {
              message = "Hello World"
           }
           headers =    {
              X-AUTH-KEY = "b84b90f0-405b-4c16-a3ae-b89e4c253ec8"
              foo = "bar"
           }
           method = "post @default(get)"
           payload-type = "json"
           url = "http://localhost:9000/rest/api"
        }
      }
     }


#### Field Description:

 * request:
    * method: HTTP method call. allowed values are
        * get
        * head
        * put
        * post
        * patch
        * delete
    * body: request body of the rest call
    * url: url to call
    * payload-type: type of payload to use. allowed types are
        * json
        * xml
        * text
    * headers: dictionary of key and values to be applied as header
 * emit-output: if the api response body is a json object emit the result back to be merged with job config. default value is false
 * allowed-status-codes: list of allowed HTTP response status codes

#### Task Output:


     {
        body =  {
         foo = "bar"
         hello = "world"
      }
        header =  {
         connection = "Keep-Alive"
         content-length = 1025
      }
        status = 200
     }

 
The **header**, **body**, **status** fields captures header, body and status of the HTTP call. if the returned
  response is of non json type, then the body is serialized as string value as shown below.
  ```
  body = "Hello world"
  ```
    
           

     

     