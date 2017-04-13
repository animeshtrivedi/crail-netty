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

package com.ibm.crail.datanode.netty.server;

import java.net.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.ibm.crail.datanode.netty.NettyConstants;
import com.ibm.crail.datanode.netty.NettyStorageTier;
import com.ibm.crail.datanode.netty.CrailNettyUtils;
import com.ibm.crail.datanode.netty.rpc.RdmaDecoderRx;
import com.ibm.crail.metadata.DataNodeStatistics;
import com.ibm.crail.storage.StorageRpcClient;
import com.ibm.crail.storage.StorageServer;
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

    public NettyStorageServer(){
        this.inetSocketAddress = getNettyDataNodeAddress();
        this.map = null;
        /* we start with 1 stag, and then every time we allocate a new block we increment it by 1 */
        this.currentStag = 1;
        this.isRunning = false;
    }

    public InetSocketAddress getAddress() {
        return this.inetSocketAddress;
    }

    private InetSocketAddress getNettyDataNodeAddress() {
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
        /* once you have it always return it */
        return NettyConstants.DATANODE_NETTY_INTERFACE;
    }

    public void run() {
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
            /* general optimization settings */
            boot.option(ChannelOption.SO_BACKLOG, 1024);
            boot.childOption(ChannelOption.SO_KEEPALIVE, true);
            this.isRunning = true;
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

    public void registerResources(StorageRpcClient storageRpcClient) throws Exception {
        int entries = (int) (NettyConstants.DATANODE_NETTY_STORAGE_LIMIT/NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE);
        map = new ConcurrentHashMap<Integer, ByteBuf>(entries);
        LOG.info("Registering resources with " + entries + " nums of " + NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE + " byte buffers");
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
            map.put(this.currentStag, buf);
            storageRpcClient.setBlock(address, (int) NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE, this.currentStag);
            LOG.info("MAP entry : " + Long.toHexString(address) +
                    " length : " + (int) NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE +
                    " stag : " + this.currentStag + " refCount: " + buf.refCnt());

            /* update counters */
            allocated += NettyConstants.DATANODE_NETTY_ALLOCATION_SIZE;
            perc=allocated * 100 / NettyConstants.DATANODE_NETTY_STORAGE_LIMIT;
            this.currentStag++;
            LOG.info("Allocation done : " + perc + "% , allocated " + allocated +
                    " / " + NettyConstants.DATANODE_NETTY_STORAGE_LIMIT);
        }
    }

    public boolean isAlive() {
        return this.isRunning;
    }

    public void join() throws Exception {

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
}
