# Crail on Netty

Crail-netty is an extension of the Crail project (www.crail.io) to enable it to run on non-RDMA networks. 
As a transport substitute, it uses the netty framework for both, RPCs, as well as data transfers. These two 
can be enabled separately. 

Crail RPCs, which happen between a client and the namenode, are defined in `com.ibm.crail.rpc.RpcBinding` interface.
This interface is implemented by `com.ibm.crail.namenode.rpc.netty.NettyNameNode` class. The data transfer part, 
which happens between a storage node and a client, is defined in `com.ibm.crail.storage` package and implemented by 
`com.ibm.crail.storage.netty.NettyStorageTier`.

**News:**

 - September 14th, 2017: Parameter `crail.storage.netty.interface`, which took the interface name, is now replaced 
 with `crail.datanode.netty.address` that takes the IP address. And command line parsing is enabled. The parameter 
 supplied at the command line take precedence over the config values set in the `crail-site.conf` file. 
 
## Building 

Apart from the dependencies of Crail (https://github.com/zrlio/crail#requirements), crail-netty depends upon netty. 
You can build the project by:
```bash
mvn -DskipTests install
```
Then copy the jar file (`crail-netty-1.0.jar`) from the `target` folder into `$CRAIL_HOME/jars/` along with the 
netty jar (`./target/crail-netty-1.0-dist/jars/netty-all-4.0.29.Final.jar`), if not there already. 

Alternatively you can also put these files in your custom classpath (if you have one) and export it.

## Configuration parameters
  * Storage Limit: This is the maximum amount of memory that crail netty will offer to save data (default: 1 GB). 
  * Allocation size: This is the chunks in which the maximum amount of memory (storage limit) will be allocate (default: 1 GB).
  * Address: The IP address or the hostname where the server should bind and run (default: hostname).
  * Port: The port where the data node should bind and run (default: 19862).
  
### Setting via `crail-site.conf` 
The current code accepts following parameters (shown here with their default values):

```bash
crail.storage.netty.storagelimit    1073741824
crail.storage.netty.allocationsize  1073741824
crail.storage.netty.address         127.0.0.1 
crail.storage.netty.port            19862
```

You should put them in the `$CRAIL_HOME/conf/crail-site.conf` file.

### Passing from the command line 
The above defined parameters can also be set via the command line with the following parameters 
```bash
  -a,--address <arg>          (string) IP address or the hostname, where to run the server  
  -l,--storageLimit <arg>     (long) storage limit
  -p,--port <arg>             (int) port number of the netty data server
  -s,--allocationSize <arg>   (long) allocation size
```
The values specified at the command prompt take precedence over the values defined in the `crail-site.conf` file. 
An example run of the netty data node would be 
```bash
$./bin/crail datanode -t com.ibm.crail.storage.netty.NettyStorageTier -- -a 10.10.10.11 -p 8881 -l $((1073741824 * 4))
```

## Enabling data transfer on Netty
Instructions to start a crail storage node is mostly similar to crail (https://github.com/zrlio/crail#deploying). 
Crail-netty implements a specific type of Crail storage node which does data transfers to a client over netty. To 
run this crail-netty strogae node: 
```bash 
$CRAIL_HOME/bin/crail datanode -t com.ibm.crail.storage.netty.NettyStorageTier
```
In order for a client to automatically pick up connection to the new storage node type, you have to add following class 
to your list of storage types types in the `$CRAIL_HOME/conf/crail-site.conf` file. You can have a comma separated 
list as (if just deploying with netty, you can delete all other types except `NettyStorageTier`) : 

```bash
crail.storage.types  com.ibm.crail.storage.rdma.RdmaStorageTier,com.ibm.crail.storage.netty.NettyStorageTier
```

## Enabling Netty RPCs

By default Crail uses DaRPC (https://github.com/zrlio/darpc) for high-performance RPC calls over RDMA-enabled networks. 
To switch RPCs from RDMA to netty, change the `crail.namenode.rpctype` in the crail config file 
(`$CRAIL_HOME/conf/crail-site.conf`): 
```bash
crail.namenode.rpctype  com.ibm.crail.namenode.rpc.NettyNameNode
```

## Setting up automatic deployment

To enable deployment via `$CRAIL_HOME/bin/start-crail.sh`, you should use `-t` flag to define netty storage node in the 
the crail slave file (`$CRAIL_HOME/conf/slave`). An example slave file might look something like this: 
```bash
hostname1 -t com.ibm.crail.storage.netty.NettyStorageTier -- -a hostname1 [...] 
hostname2 -t com.ibm.crail.storage.netty.NettyStorageTier -- -a hostname2  [...]
hostname3 -t com.ibm.crail.storage.netty.NettyStorageTier -- -a hostname3  [...]
...
```
[...] represents other paramters that can be specificed on the shell (`-a,` `-p`, `-s`, `-l`). IMPORTANT: notice the separation of the arguments with `--` in the slave files. 

## An example of netty-only Crail deployment on localhost 
Here is the content of `core-site.xml` and `crail-site.conf` when you just want to configure crail to run on localhost
using netty
 
`core-site.xml` 

```bash
<configuration>
  <property>
   <name>fs.crail.impl</name>
   <value>com.ibm.crail.hdfs.CrailHadoopFileSystem</value>
  </property>
  <property>
    <name>fs.defaultFS</name>
    <value>crail://localhost:9060</value>
  </property>
  <property>
    <name>fs.AbstractFileSystem.crail.impl</name>
    <value>com.ibm.crail.hdfs.CrailHDFS</value>
  </property>
  <property>
    <name>io.file.buffer.size</name>
    <value>1048576</value>
  </property>
</configuration>
 
```

`crail-site.conf`
```bash 
crail.blocksize   1048576
crail.buffersize  1048576
crail.regionsize  1073741824
crail.cachelimit  1073741824
crail.cachepath   /tmp/
crail.singleton   true
crail.statistics  true

crail.namenode.address         crail://localhost:9060
crail.namenode.blockselection  roundrobin
crail.namenode.rpctype        com.ibm.crail.namenode.rpc.netty.NettyNameNode

crail.storage.types com.ibm.crail.storage.netty.NettyStorageTier
crail.storage.netty.storagelimit       1073741824
crail.storage.netty.allocationsize     1073741824
crail.storage.netty.address            127.0.0.1
crail.storage.netty.port               19862
```
## A word on performance 
The crail-netty port meant to facilitate experience and testing of crail file system for non-RDMA networks. Clearly, 
crail-netty cannot deliver the same level of performance as RDMA-enabled crail deployments. Your performance mileage 
will vary depends upon your TCP and netty settings. Please let us know about your performance experiences.

**NOTE:** We recommend to use netty version 4.1.5. Our maven build is for 4.0.29, which is the same version used in 
Apache Spark. This settings facilitates an easy deployment with Spark with crail's `spark-io` 
(https://github.com/zrlio/spark-io) plugins. 

## Contributions

PRs are always welcome. Please fork, and make necessary modifications you propose, and let us know. 

## Contact 

If you have questions or suggestions, feel free to post at:

https://groups.google.com/forum/#!forum/zrlio-users

or email: zrlio-users@googlegroups.com
