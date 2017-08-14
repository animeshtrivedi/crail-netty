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

package com.ibm.crail.storage.netty;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.StorageServer;
import com.ibm.crail.storage.StorageTier;
import com.ibm.crail.storage.netty.client.NettyEndpointGroup;
import com.ibm.crail.storage.netty.server.NettyStorageServer;
import com.ibm.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;

public class NettyStorageTier extends StorageTier {
    static private final Logger LOG = CrailNettyUtils.getLogger();
    /* Datanode client-side common stuff */
    NettyEndpointGroup epGroup;

    /* this called from the client side as well, dont do any init here */
    public NettyStorageTier(){
        epGroup = null;
    }

    final public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
        NettyConstants.init(crailConfiguration);
    }

    @Override
    final public StorageServer launchServer() throws Exception {
        NettyStorageServer nettyServer = new NettyStorageServer();
        Thread dataNode = new Thread(nettyServer);
        dataNode.start();
        LOG.info("launched netty storage server ...");
        return nettyServer;
    }

    final public StorageEndpoint createEndpoint(DataNodeInfo dataNodeInfo) throws IOException{
        InetSocketAddress addr = CrailUtils.datanodeInfo2SocketAddr(dataNodeInfo);
        LOG.debug(" Opening a connection to StorageNode: " + addr.toString());
        /* we protect the call for init of the common framework */
        synchronized(this) {
            /* this is kind of ugly as I am tying to save the cost of allocating the end point group on the
            data node server side.
             */
            if (epGroup == null) {
                epGroup = new NettyEndpointGroup();
            }
        }
        return epGroup.createEndpoint(addr);
    }

    final public void close() throws Exception {
        synchronized (this) {
            if (this.epGroup != null) {
                this.epGroup.close();
                this.epGroup = null;
            }
        }
    }

    final public void printConf(Logger logger) {
        logger.info(NettyConstants.STORAGENODE_NETTY_STORAGE_LIMIT_KEY + " " + NettyConstants.STORAGENODE_NETTY_STORAGE_LIMIT);
        logger.info(NettyConstants.STORAGENODE_NETTY_ALLOCATION_SIZE_KEY + " " + NettyConstants.STORAGENODE_NETTY_ALLOCATION_SIZE);
        logger.info(NettyConstants.STORAGENODE_NETTY_INTERFACE_KEY + " " + NettyConstants.STORAGENODE_NETTY_INTERFACE);
    }
}
