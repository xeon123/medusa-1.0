# Overview

This is a project that manages Hadoop MR instances in several clusters.
This app manage the execution of the job and check cloud liveness. This also tolerate arbitrary faults.

This manual is a sum up of small notes, and some of them are hard to understand.

## Installation of the Proxy

The `emulab-install.sh` script install all the packages in a clean state OS. These packages are Hadoop MR, python2.7 and java.

    hadoop-coc-1:~/Programs/medusa-1.0# ./config-scripts/emulab-install.sh

To install all necessary python plugins in the virtualenv do this:

    hadoop-coc-1:~/Programs/medusa-1.0# source medusa-env/bin/activate
    (medusa-env)hadoop-coc-1:~/Programs/medusa-1.0# ./config-scripts/pip-plugins.sh

## Configuration of MapReduce

To configure Hadoop MR configuration files you need to set the slaves in the Hadoop runtime. E.g.:

    hadoop-coc-1:~/Programs/medusa-1.0# cat ~/Programs/hadoop/etc/hadoop/slaves
	hadoop-coc-2
	hadoop-coc-3
	hadoop-coc-4

If you still need to set the SSH password, you need to import them so that SSH became passwordless. E.g.:

    hadoop-coc-1# cat .ssh/id_rsa.pub | ssh sheena@host10 'cat >> .ssh/authorized_keys'

Then you must configure Hadoop MR files. E.g.:

    hadoop-coc-1:~/Programs/medusa-1.0# ./config-scripts/onfigure-files.sh hadoop00

Verify if the /etc/hosts have the correct hostnames. E.g.:

    hadoop-coc-1:~# cat /etc/hosts
    (...)

    172.16.100.1	hadoop-coc-1
    172.16.100.2	hadoop-coc-2
    172.16.100.3	hadoop-coc-3
    172.16.100.4	hadoop-coc-4


Then you need to format the HDFS before starting Hadoop MR:

    hadoop-coc-1:~# hadoop namenode -format
    hadoop-coc-1:~# start-all.sh


## Creating python environment

This tool has the purpose to control the execution of the mapreduce job in several clusters.
We tested the proxy in python 2.7, and fabric 1.6.0.
It is preferable that you install using a isolated Python environment using `virtualenv`.

Here it is an example of creating an `virtualenv` environment.

    virtualenv ENV (e.g.$ virtualenv python-2.7) 
    ENV/bin/activate

All the clusters must be connected to an Rabbit MQ server.
All the clusters must know each other and added to authorized keys to allow ssh passwordless connections (The clusters that I am using know already)

You can install the python plugins that are set in `pip-plugins.sh`

## Hadoop MapReduce

To run Hadoop MR, you need it to configure it first. 
If you need help to install MapReduce, you can this link [1](https://raseshmori.wordpress.com/2012/10/14/install-hadoop-nextgen-yarn-multi-node-cluster/).
I also have set the `.bashrc` with some environment variables.

    # User specific aliases and functions
    export HADOOP_HOME=/home/pcosta/Programs/hadoop
    export HADOOP_MAPRED_HOME=$HADOOP_HOME
    export HADOOP_COMMON_HOME=$HADOOP_HOME
    export HADOOP_HDFS_HOME=$HADOOP_HOME
    export YARN_HOME=$HADOOP_HOME
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
    export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
    export HADOOP_MAPREDUCE=~/Programs/hadoop-1.0.4

If the hosts are behind a firewall and need to transfer HDFS data in he web, in is necessary to configure HDFS to use the protocol HFFPFS. 
Follow this link [3](https://hadoop.apache.org/docs/r2.0.4-alpha/hadoop-hdfs-httpfs/ServerSetup.html).
	
## Rabbit MQ

You need also to install RabbitMQ with `sudo apt-get install rabbitmq-server`, or install it in the `$HOME` path.

I show below a small and extremely lightweight demo of using Celery for distributed computation.
It is necessary to configure some environment variables in RabbitMQ as `root` before running it.

    $ rabbitmqctl add_user celeryuser celery
    $ rabbitmqctl add_vhost celeryvhost
    $ rabbitmqctl set_permissions -p celeryvhost celeryuser ".*" ".*" ".*"
    $ rabbitmq-plugins enable rabbitmq_management (guest/guest)
    (Note: the user/password configuration here must match celeryconfig.py.)

To starting RabbitMQ, run this command `rabbitmq-server`.

Each celery instance is connected to the RabbitMQ using queues.
Each host must have their own queue, and in this use case, 2 hosts are connected to the same broker.
The addresses of the hosts are:

    hadoop-coc-1: 10.101.79.122
    hadoop-coc-2: 10.101.79.123

It is necessary to configure `celeryconfig.py`. An example is provided below.

### Example of configuration of the `celeryconfig.py`

    import os
    import sys

    # add hadoop python to the env, just for the running
    sys.path.append(os.path.dirname(os.path.basename(__file__)))

    # broker configuration
    BROKER_URL = "amqp://celeryuser:celery@192.168.56.100:5672/celeryvhost"

    CELERY_RESULT_BACKEND = "amqp"

    # for debug
    #CELERY_ALWAYS_EAGER = True

    # module loaded
    CELERY_IMPORTS = ("manager.mergedirs", "manager.system", "manager.utility", "manager.pingdaemon", "manager.hdfs")

The settings are defined in a json file

    {
      "config": {
        "localhome": "/home/xeon",
        "hadoophome": "/home/xeon/Programs/hadoop",
        "javahome": "/usr/lib/jvm/java-1.6.0-openjdk",
        "hadoopmanager": "/repositories/git/medusa-1.0",
        "namenode": "localhost:9000"
      }
    }


### To run the unit tests:

The unit tests were run in setup environments and in distinct conditions. No integration tests were made.
Even though, you still want to run all tests install nose.

    pip install nose 

Now you can run tests for your project:

    cd path/to/project
    nosetests


To run the tests it is necessary to set configuration parameters, like `HADOOP_HOME`. They are defined in `settings.py`.


### Run everything

Before you run everything, you need to create several symbolic links.

    ~/Programs/medusa-1.0/manager/manager$ ln -s server_settings.py settings.py


### To configure the prediction algorithm it is necessary to install and run some programs.

1. Run `./config-scripts/configure-server.sh`
2. Run `iperf` in the server and the client side.
3. `iperf-server.sh` - server side
4. `iperf-client.sh` Node01-4 Node01-0 400; iperf-client.sh Node01-4 Node01-0 800; iperf-client.sh Node01-4 Node01-0 1200
5. check if exist the files `$MANAGER_HOME/temp/{ clusters.json, Node01-4-Node01-0-1200.csv, networkork_data,   Node01-4-Node01-0-400.csv, Node01-0-Node01-4-800.csv, job_log.json, prediction.json }`
6. Create the link to the settings file. `~/repositories/git/medusa-1.0/manager/manager$ ln -s client_settings.py settings.py`
7. You need to set `CLUSTERS` variable in the `settings.py`
8. You can test if the celery is running properly running the `testHello.py`.


## QUICK TEST:

To check if everything is running, run the test `testHello.py` and check if the output are the hostnames that are connected to the Rabbit MQ server.

## Q`&`A
### 1. I have problems initializing `celery`

If you have problems initializing `./bin/celery-debug.sh`, check the log `less ~/Programs/rabbitmq_server-3.1.0/var/log/rabbitmq/rabbit@adminuser-VirtualBox-073n.log`.

        $ rabbitmqctl add_user celeryuser celery
        $ rabbitmqctl add_vhost celeryvhost
        $ rabbitmqctl set_permissions -p celeryvhost celeryuser ".*" ".*" ".*"

### 2. I have the node down

If you have the error that indicates that the node is down,

    Status of node 'rabbit@xxx' ...
    Error: unable to connect to node 'rabbit@xxx': nodedown
    diagnostics:
    - nodes and their ports on xxx: [{rabbitmqctl,...}]
    - current node: 'rabbitmqctlxxx@xxx'
    - current node home dir: [...]
    - current node cookie hash: [...]


verify that .erlang.cookie from the rabbitmq_server-3.1.0 is the same as the one in the $HOME. E.g., 

    rabbitmq_server-3.1.0]$ ln -s ~/.erlang.cookie .


### 3. How to configure rabbitMQ in Amazon EC2?

To configure rabbitMQ in Amazon EC2, set

    ubuntu@ip-10-170-74-198:~$ cat /etc/rabbitmq/rabbitmq-env.conf
        RABBITMQ_NODE_IP_ADDRESS=0.0.0.0

DNS translates names into IP addresses. 0.0.0.0 means "listen on all network interfaces", so whatever
IP address public DNS resolves to, is used. In fact, there's only one IP address per instance most of the time.

Due to the private and public DNS, the queues/channels assume the private DNS, despite the host is configured to access the public DNS. E.g., 

    Broker: RABBITMQ_NODE_IP_ADDRESS=0.0.0.0
    Host EU: BROKER_HOST = "54.235.41.197"<- broker public DNS
    Client: CLUSTERS=["ip-10-210-130-219"]<- Host EU private DNS


### 4. How to configure HTTPFS?

To configure the HTTPFS in the clusters, the following was added in the core-site.xml (http://hadoop.apache.org/docs/current/hadoop-hdfs-httpfs/ServerSetup.html) and restart hadoop.


    <property> <name>hadoop.proxyuser.ubuntu.hosts</name> <value>host-ec2.eu-west-1.compute.amazonaws.com</value> </property>
    <property> <name>hadoop.proxyuser.ubuntu.groups</name> <value>\*</value> </property>


If you have this error, verify that you have this in core-site.xml [1](http://jayatiatblogs.blogspot.pt/2011/05/oozie-installation.html) "ubuntu cannot impersionate ubuntu". You need to restart hadoop.


    <property> <name>hadoop.proxyuser.ubuntu.hosts</name> <value>\*</value> </property>



    13/07/28 14:26:29 ERROR tools.DistCp: Exception encountered 
    org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.ipc.RemoteException): User: ubuntu is not allowed to impersonate ubuntu
        at org.apache.hadoop.hdfs.web.JsonUtil.toRemoteException(JsonUtil.java:169)
        at org.apache.hadoop.hdfs.web.WebHdfsFileSystem.validateResponse(WebHdfsFileSystem.java:283)
        at org.apache.hadoop.hdfs.web.WebHdfsFileSystem.access$500(WebHdfsFileSystem.java:91)
        at org.apache.hadoop.hdfs.web.WebHdfsFileSystem$Runner.getResponse(WebHdfsFileSystem.java:549)
        at org.apache.hadoop.hdfs.web.WebHdfsFileSystem$Runner.run(WebHdfsFileSystem.java:470)
        at org.apache.hadoop.hdfs.web.WebHdfsFileSystem.run(WebHdfsFileSystem.java:403)
        at org.apache.hadoop.hdfs.web.WebHdfsFileSystem.getHdfsFileStatus(WebHdfsFileSystem.java:570)
        at org.apache.hadoop.hdfs.web.WebHdfsFileSystem.getFileStatus(WebHdfsFileSystem.java:581)
        at org.apache.hadoop.fs.FileSystem.isFile(FileSystem.java:1371)
        at org.apache.hadoop.tools.SimpleCopyListing.validatePaths(SimpleCopyListing.java:67)
        at org.apache.hadoop.tools.CopyListing.buildListing(CopyListing.java:79)
        at org.apache.hadoop.tools.GlobbedCopyListing.doBuildListing(GlobbedCopyListing.java:90)
        at org.apache.hadoop.tools.CopyListing.buildListing(CopyListing.java:80)
        at org.apache.hadoop.tools.DistCp.createInputFileListing(DistCp.java:326)
        at org.apache.hadoop.tools.DistCp.execute(DistCp.java:151)
        at org.apache.hadoop.tools.DistCp.run(DistCp.java:118)
        at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:70)
        at org.apache.hadoop.tools.DistCp.main(DistCp.java:374)

 
 ##### 5. How to set internal ip address in ExoGeni?

 In ExoGeni, NEuca service sets the loopback address to the hostname. Thus, we cannot assign the internal ip to the hostname. See this [link](https://groups.google.com/forum/#!topic/geni-users/abFbD-18i4I)


    by default, the NEuca writes a loopback address to the hostname[1]. For me this is a problem because, I would like to assign the ip address 172.16.100.1 to the hostname, but now I can't. Is there a workaroud to assign 172.16.100.1 ip to the hostname?

    [1]

    $ cat /etc/hosts

    127.0.0.1	localhost
    127.0.1.1	ubuntu

    ### BEGIN NEuca loopback modifications - DO NOT EDIT BETWEEN THESE LINES. ###
    127.255.255.1	NodeGroup0-0
    ### END NEuca loopback modifications - DO NOT EDIT BETWEEN THESE LINES. ###

**Answer:**

Take a look in the following configuration file `/etc/neuca/config`, and you will find a section that looks like this:

    [runtime]
    ## Set the node name in /etc/hosts to a value in the loopback space.
    ## Value can be "true" or "false"
    #set-loopback-hostname = true
    #
    ## The address that should be added to /etc/hosts if "set-loopback-hostname" is "true"
    ## This address *must* be in the 127.0.0.0/8 space; any other value will result in an error.
    #loopback-address = 127.255.255.1

You can disable setting the hostname to loopback, if you like, by uncommenting the appropriate configuration item, and altering it to taste.
After you have done so, restart the neuca daemon thus `/etc/init.d/neuca restart`
Afterward, you can edit the `/etc/hosts` file to your heart’s content.


### 6. Can I run celery as `root`?
Don’t Run Celery as the Root User. Check this [link](http://www.rabbitmq.com/tutorials/tutorial-one-python.html)

### 7. How to set JAVA_HOME for hadoop?
Despite all the configuration, when we start mapreduce, we can get a problem in the `JAVA_HOME`, we can set the variable in `./etc/hadoop/yarn-site.sh` and `./etc/hadoop/hadoop-env.sh`.

    Error: JAVA_HOME is not set and could not be found.

