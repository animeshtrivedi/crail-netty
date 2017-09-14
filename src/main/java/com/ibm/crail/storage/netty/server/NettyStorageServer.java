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

package com.ibm.crail.storage.netty.server;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.storage.StorageResource;
import com.ibm.crail.storage.StorageServer;
import com.ibm.crail.storage.netty.CrailNettyUtils;
import com.ibm.crail.storage.netty.NettyConstants;
import com.ibm.crail.storage.netty.rpc.RdmaDecoderRx;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import sun.nio.ch.DirectBuffer;

import java.net.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static io.netty.buffer.Unpooled.directBuffer;


/**
 * Created by atr on 09.08.16.
 */
public class NettyStorageServer implements Runnable, StorageServer {
    private static final Logger LOG = CrailNettyUtils.getLogger();
    private InetSocketAddress inetSocketAddress;
    private ConcurrentHashMap<Integer, ByteBuf> map;
    private int currentStag;
    private boolean isRunning;
    private long allocated;

    public NettyStorageServer(){
        //Don't do anything here as the init with the configuration is called later in the code
    }

    final public InetSocketAddress getAddress() {
        return this.inetSocketAddress;
    }

    private void initServer() throws Exception {
        this.inetSocketAddress = getNettyDataNodeAddress();
        this.map = null;
        /* we start with 1 stag, and then every time we allocate a new block we increment it by 1 */
        this.currentStag = 1;
        this.isRunning = false;
        this.allocated = 0;
        int entries = (int) (NettyConstants.STORAGENODE_NETTY_STORAGE_LIMIT /NettyConstants.STORAGENODE_NETTY_ALLOCATION_SIZE);
        this.map = new ConcurrentHashMap<Integer, ByteBuf>(entries);
        LOG.info(" constructor, alloc size " + NettyConstants.STORAGENODE_NETTY_ALLOCATION_SIZE +
                " limit " + NettyConstants.STORAGENODE_NETTY_STORAGE_LIMIT +
                " gives #entries " + entries);
    }

    private InetSocketAddress getNettyDataNodeAddress() throws Exception {
        if(null == NettyConstants.STORAGENODE_NETTY_INTERFACE) {
                /* we need to init it */
            NetworkInterface netif = null;
            try {
                netif = NetworkInterface.getByName(NettyConstants._ifname);
            } catch (SocketException e) {
                e.printStackTrace();
            }
            if(netif == null ) {
                throw new Exception("Expected interface " + NettyConstants._ifname + " not found. Please check you crail config file");
            }
            List<InterfaceAddress> addresses = netif.getInterfaceAddresses();
            InetAddress addr = null;
            for (InterfaceAddress address: addresses){
                if (address.getBroadcast() != null){
                    addr = address.getAddress();
                }
            }
            NettyConstants.STORAGENODE_NETTY_INTERFACE = new InetSocketAddress(addr,
                    NettyConstants.STORAGENODE_NETTY_PORT);
        }
        /* once you have it always return it */
        return NettyConstants.STORAGENODE_NETTY_INTERFACE;
    }

    final public void run() {
        /* start the netty server */
        EventLoopGroup acceptGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap boot = new ServerBootstrap();
            boot.group(acceptGroup, workerGroup);
            /* we use sockets */
            boot.channel(NioServerSocketChannel.class);
            /* for new incoming connection */
            final NettyStorageServer currentObj = this;
            boot.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    LOG.info("TID: " + Thread.currentThread().getId() +
                            " , a new client connection has arrived from : " + ch.remoteAddress().toString());
                            /* incoming pipeline */
                    ch.pipeline().addLast(
                            new RdmaDecoderRx(), /* this makes full RDMA messages */
                            new IncomingRequestHandler(ch, currentObj));
                            /* outgoing pipeline */
                    //ch.pipeline().addLast(new RdmaEncoderTx());
                }
            });
            this.isRunning = true;
            /* general optimization settings */
            boot.option(ChannelOption.SO_BACKLOG, 1024);
            boot.childOption(ChannelOption.SO_KEEPALIVE, true);
            /* now we bind the server and start */
            ChannelFuture f = boot.bind(this.inetSocketAddress.getAddress(),
                    this.inetSocketAddress.getPort()).sync();
            LOG.info("NettyStorageServer is binded to : " + this.inetSocketAddress);
            /* at this point we are binded and ready */
            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
            acceptGroup.shutdownGracefully();
            LOG.info("NettyStorageServer at " + this.inetSocketAddress + " is shutdown");
            this.isRunning = false;
        }
    }

    final public StorageResource allocateResource() throws Exception {
        double perc;
        StorageResource res = null;
        /* Have we already allocated all? then return null */
        if(allocated < NettyConstants.STORAGENODE_NETTY_STORAGE_LIMIT) {
            /* allocate a new buffer */
            ByteBuf buf = directBuffer((int) NettyConstants.STORAGENODE_NETTY_ALLOCATION_SIZE,
                    (int) NettyConstants.STORAGENODE_NETTY_ALLOCATION_SIZE);
            /* retain this buffer */
            buf.retain();
            Long address = ((DirectBuffer) buf.nioBuffer()).address();
            /* update entries */
            map.put(this.currentStag, buf);
            res = StorageResource.createResource(address,
                    (int) NettyConstants.STORAGENODE_NETTY_ALLOCATION_SIZE,
                    this.currentStag);
            LOG.info("MAP entry : " + Long.toHexString(address) +
                    " length : " + (int) NettyConstants.STORAGENODE_NETTY_ALLOCATION_SIZE +
                    " stag : " + this.currentStag + " refCount: " + buf.refCnt());
            /* update counters */
            allocated += NettyConstants.STORAGENODE_NETTY_ALLOCATION_SIZE;
            perc=allocated * 100 / NettyConstants.STORAGENODE_NETTY_STORAGE_LIMIT;
            this.currentStag++;
            LOG.info("Allocation done : " + perc + "% , allocated " + allocated +
                    " / " + NettyConstants.STORAGENODE_NETTY_STORAGE_LIMIT);
        }
        return res;
    }

    final public boolean isAlive() {
        //TODO: there is a race here between launchServer and checking of alive. This needs to be cleaned properly.
        return true;
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

    public void init(CrailConfiguration crailConfiguration, String[] strings) throws Exception {
        NettyConstants.init(crailConfiguration);
        initServer();
    }

    public void printConf(Logger logger) {
        logger.info(" NETTY: " + NettyConstants.STORAGENODE_NETTY_STORAGE_LIMIT_KEY + " : " + NettyConstants.STORAGENODE_NETTY_STORAGE_LIMIT);
        logger.info(" NETTY: " + NettyConstants.STORAGENODE_NETTY_ALLOCATION_SIZE_KEY + " : " + NettyConstants.STORAGENODE_NETTY_ALLOCATION_SIZE);
        logger.info(" NETTY: " + NettyConstants.STORAGENODE_NETTY_INTERFACE_KEY + " : " + NettyConstants.STORAGENODE_NETTY_INTERFACE);
    }
}

