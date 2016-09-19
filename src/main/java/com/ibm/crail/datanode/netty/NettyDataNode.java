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

package com.ibm.crail.datanode.netty;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.datanode.DataNode;
import com.ibm.crail.datanode.DataNodeEndpoint;
import com.ibm.crail.datanode.netty.client.NettyEndpointGroup;
import com.ibm.crail.datanode.netty.server.NettyServer;
import com.ibm.crail.namenode.protocol.DataNodeStatistics;

import io.netty.buffer.ByteBuf;

import org.slf4j.Logger;

import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.net.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static io.netty.buffer.Unpooled.directBuffer;

public class NettyDataNode extends DataNode {
    static private final Logger LOG = CrailNettyUtils.getLogger();
    /* Datanode server-side data structures */
    private ConcurrentHashMap<Integer, ByteBuf> map;
    private int stag;
    private NettyServer datanode;

    /* Datanode client-side stuff */
    NettyEndpointGroup epGroup;

    /* this called from the client side as well, dont do any init here */
    public NettyDataNode(){
        epGroup = null;
        datanode = null;
    }

    /**
     * Look up and return an associated ByteBuffer for a given key
     *
     * @param stag The stag for which to look up the buffer.
     *
     * @return ByteBuffer or NULL
     */
    public ByteBuf stagToNettyBuffer(int stag) {
        return map.getOrDefault(stag, null);
    }

    @Override
    public DataNodeEndpoint createEndpoint(InetSocketAddress inetAddress) throws IOException{
        if(epGroup == null) {
            epGroup = new NettyEndpointGroup();
        }
        return epGroup.createEndpoint(inetAddress);
    }

    @Override
    public void run() throws Exception{

        int entries = (int) (NettyConstants.DATANODE_NETTY_STORAGE_LIMIT/NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE);
        map = new ConcurrentHashMap<Integer, ByteBuf>(entries);
        datanode = null;
        /* we start with stag 1 and increment it constantly */
        stag = 1;
        LOG.info("Booting with " + entries + " nums of " + NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE + " byte buffers");

        /* this manages the netty datanode which processes the client requests */
        datanode = new NettyServer(getAddress(), this);
        datanode.start();

        /* now the Namenode Processor communication part */
        long allocated = 0;
        double perc;
        LOG.info("Allocation started for the target of : " + NettyConstants.DATANODE_NETTY_STORAGE_LIMIT);
        while(allocated < NettyConstants.DATANODE_NETTY_STORAGE_LIMIT) {
            /* allocate a new buffer */
            ByteBuf buf = directBuffer((int) NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE,
                    (int) NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE);
            /* retain this buffer */
            buf.retain();
            Long address = ((DirectBuffer) buf.nioBuffer()).address();

            /* update entries */
            map.put(this.stag, buf);
            this.setBlock(address, (int) NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE, this.stag);
            LOG.info("MAP entry : " + Long.toHexString(address) +
                    " length : " + (int) NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE +
                    " stag : " + this.stag + " refCount: " + buf.refCnt());

            /* update counters */
            allocated += NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE;
            perc=allocated *100 / NettyConstants.DATANODE_NETTY_STORAGE_LIMIT;
            this.stag++;
            LOG.info("Allocation done : " + perc + "% , allocated " + allocated +
                    " / " + NettyConstants.DATANODE_NETTY_STORAGE_LIMIT);
        }

        while (true) {
            DataNodeStatistics statistics = this.getDataNode();
            LOG.info("Datanode statistics, freeBlocks " + statistics.getFreeBlockCount());
            Thread.sleep(2000);
        }

        /* now we wait for the other thread */
        //datanode.join();
    }

    @Override
    public void close() throws Exception {
        if(this.epGroup != null) {
            this.epGroup.close();
        }
    }

    @Override
    public InetSocketAddress getAddress() {
        synchronized (this) {
            if(null == NettyConstants.DATANODE_NETTY_INTERFACE) {
                /* we need to init it */
                NetworkInterface netif = null;
                try {
                    netif = NetworkInterface.getByName(NettyConstants._ifname);
                } catch (SocketException e) {
                    e.printStackTrace();
                }
                if(netif == null ) {
                    LOG.error("Expected interface " + NettyConstants._ifname + " not found. Please check you crail config file");
                    return null;
                }
                List<InterfaceAddress> addresses = netif.getInterfaceAddresses();
                InetAddress addr = null;
                for (InterfaceAddress address: addresses){
                    if (address.getBroadcast() != null){
                        addr = address.getAddress();
                    }
                }
                NettyConstants.DATANODE_NETTY_INTERFACE = new InetSocketAddress(addr,
                        NettyConstants.DATANODE_NETTY_PORT);
            }
        }
        /* once you have it always return it */
        return NettyConstants.DATANODE_NETTY_INTERFACE;
    }

    @Override
    public void printConf(Logger logger) {
        logger.info(NettyConstants.DATANODE_NETTY_STORAGE_LIMIT_KEY + " " + NettyConstants.DATANODE_NETTY_STORAGE_LIMIT);
        logger.info(NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE_KEY + " " + NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE);
        logger.info(NettyConstants.DATANODE_NETTY_INTERFACE_KEY + " " + NettyConstants.DATANODE_NETTY_INTERFACE);
    }

    @Override
    public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
        NettyConstants.init(crailConfiguration);
    }
}
