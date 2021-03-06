/*
 * Crail-Netty: An implementation of Crail DataNode and RPC interfaces
 *              to run on netty/TCP transport.
 *
 * Author: Animesh Trivedi <atr@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.namenode.rpc.netty;

import com.ibm.crail.namenode.rpc.netty.client.NettyRpcNamenodeClient;
import com.ibm.crail.rpc.RpcBinding;
import com.ibm.crail.rpc.RpcNameNodeService;
import com.ibm.crail.rpc.RpcServer;

public class NettyNameNode extends NettyRpcNamenodeClient implements RpcBinding {
    public NettyNameNode(){
    }
    public RpcServer launchServer(RpcNameNodeService rpcNameNodeService) {
        return new NettyNamenodeServer(rpcNameNodeService);
    }
}
