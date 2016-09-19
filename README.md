# Crail on Netty

Crail-netty is an extension of the crail file system project to enable 
it to run on non-RDMA networks. As a transport substitute, it uses the 
netty framework for both, RPCs, as well as data transfers. These two 
can be enabled separately. 

Crail RPCs, which happen between a client and the namenode, are defined 
by `interface com.ibm.crail.namenode.rpc.RpcNameNode` and implemented in 
`com.ibm.crail.namenode.rpc.netty.NettyNameNode`. And the data transfer 
part, which happens between a datanode and a client, is defined in 
`abstract class com.ibm.crail.datanode.DataNode` and implemented by 
`com.ibm.crail.datanode.netty.NettyDataNode`.

## Building 

Apart from the dependencies of crail (https://github.com/zrlio/crail#requirements),
crail-netty depends upon netty. You can build the project by:
```bash
mvn -DskipTests install
```
Then copy the jar file (`crail-netty-1.0.jar`) from the `target` folder 
into `$CRAIL_HOME/jars/` along with the netty jar (`netty-all-4.0.29.Final.jar`),
if not there already. 

Alternatively you can also put these files in your custom classpath 
(if you have one) and export it.

## Configuration parameters
The current code accepts following parameters (shown here with their 
default values):
```
crail.datanode.netty.storagelimit    1073741824
crail.datanode.netty.allocationsize  1073741824
crail.datanode.netty.interface       eth0  
crail.datanode.netty.port            19862
```

You should put them in the `$CRAIL_HOME/conf/crail-site.conf` file.

**Note:** Currently these values cannot be overidden by a command line 
parameter. We will support this feature with the next release.

## Enabling data transfer on netty
Instructions to start a crail datanode is mostly similar to crail
(https://github.com/zrlio/crail#deploying). Crail-netty implements a 
specific type of crail datanode which does data transfers to a client 
over netty. To run this crail-netty datanode:
```bash 
$CRAIL_HOME/bin/crail datanode -t com.ibm.crail.datanode.netty.NettyDataNode
```
In order for a client to automatically pick up connection to the new 
datanode type, you have to add following class to your list of datanode 
types in the `$CRAIL_HOME/conf/crail-site.conf` file. An example of 
such entry is : 

```bash
crail.datanode.types  com.ibm.crail.datanode.rdma.RdmaDataNode,com.ibm.crail.datanode.netty.NettyDataNode
```

Please note that, this is a comma separated list of datanode **types** 
which defines the priority order as well in which the blocks from 
datanodes are consumed by the namenode. 

## Enabling netty RPCs

By default crail uses DaRPC (https://github.com/zrlio/darpc) for 
high-performance RPC calls over RDMA-enabled networks. To switch RPCs 
from RDMA to netty, change the `crail.namenode.rpc.type` in the crail 
config file (`$CRAIL_HOME/conf/crail-site.conf`): 
```bash
crail.namenode.rpc.type  com.ibm.crail.namenode.rpc.NettyNameNode
```

## Setting up automatic deployment

To enable deployment via `$CRAIL_HOME/bin/start-crail.sh`, you should 
use `-t` flag to define netty datanodes in the the crail slave file 
(`$CRAIL_HOME/conf/slave`). An example slave file might look something 
like this: 
```bash
hostname1 -t com.ibm.crail.datanode.netty.NettyDataNode
hostname2 -t com.ibm.crail.datanode.netty.NettyDataNode
hostname3 -t com.ibm.crail.datanode.netty.NettyDataNode
...
```
## A word on performance 
The crail-netty port meant to facilitate experience and testing of crail 
file system for non-RDMA networks. Clearly, crail-netty cannot deliver 
the same level of performance as RDMA-enabled crail deployments. Your 
performance mileage will vary depends upon your TCP and netty settings.
Please let us know about your performance experiences.

**NOTE:** We recommend to use netty version 4.1.5. Our maven build is 
for 4.0.29, which is the same version used in Apache Spark. This 
settings facilitates an easy deployment with Spark with 
crail's `spark-io` (https://github.com/zrlio/spark-io) plugins. 

## Contributions

PRs are always welcome. Please fork, and make necessary modifications 
you propose, and let us know. 

## Contact 

If you have questions or suggestions, feel free to post at:

https://groups.google.com/forum/#!forum/zrlio-users

or email: zrlio-users@googlegroups.com
