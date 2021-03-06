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

import java.net.InetSocketAddress;
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
        NettyConstants conf = NettyConstants.get();
        this.inetSocketAddress = conf.getNettyDataNodeAddress();
        assert this.inetSocketAddress != null;
        this.map = null;
        /* we start with 1 stag, and then every time we allocate a new block we increment it by 1 */
        this.currentStag = 1;
        this.isRunning = false;
        this.allocated = 0;
        int entries = (int) (conf.getStorageLimit() / conf.getAllocationSize());
        this.map = new ConcurrentHashMap<Integer, ByteBuf>(entries);
        LOG.info(" constructor, alloc size " + conf.getAllocationSize() +
                " limit " + conf.getStorageLimit() +
                " gives #entries " + entries);
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
        NettyConstants conf = NettyConstants.get();
        /* Have we already allocated all? then return null */
        if(allocated < conf.getStorageLimit()) {
            /* allocate a new buffer */
            ByteBuf buf = directBuffer((int) conf.getAllocationSize(),
                    (int) conf.getAllocationSize());
            /* retain this buffer */
            buf.retain();
            Long address = ((DirectBuffer) buf.nioBuffer()).address();
            /* update entries */
            map.put(this.currentStag, buf);
            res = StorageResource.createResource(address,
                    (int) conf.getAllocationSize(),
                    this.currentStag);
            LOG.info("MAP entry : " + Long.toHexString(address) +
                    " length : " + (int) conf.getAllocationSize()+
                    " stag : " + this.currentStag + " refCount: " + buf.refCnt());
            /* update counters */
            allocated += conf.getAllocationSize();
            perc=allocated * 100 / conf.getStorageLimit();
            this.currentStag++;
            LOG.info("Allocation done : " + perc + "% , allocated " + allocated +
                    " / " + conf.getStorageLimit());
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
        NettyConstants.get().init(crailConfiguration, strings);
        initServer();
    }

    public void printConf(Logger logger) {
        logger.info("\n"+ NettyConstants.get().printConf());
    }
}

