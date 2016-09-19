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

import java.net.InetSocketAddress;

import com.ibm.crail.datanode.netty.NettyDataNode;
import com.ibm.crail.datanode.netty.CrailNettyUtils;
import com.ibm.crail.datanode.netty.rpc.RdmaDecoderRx;
import io.netty.bootstrap.ServerBootstrap;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;


/**
 * Created by atr on 09.08.16.
 */
public class NettyServer extends Thread {
    private static final Logger LOG = CrailNettyUtils.getLogger();
    private InetSocketAddress inetSocketAddress;
    private NettyDataNode dataNode;

    public NettyServer(InetSocketAddress i, NettyDataNode dataNode){
        this.inetSocketAddress = i;
        this.dataNode= dataNode;
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
            boot.childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            LOG.info("TID: " + Thread.currentThread().getId() + " , a new client connection has arrived from : " + ch.remoteAddress().toString());
                            /* incoming pipeline */
                            ch.pipeline().addLast(
                                    new RdmaDecoderRx(), /* this makes full RDMA messages */
                                    new IncomingRequestHandler(ch, dataNode));
                            /* outgoing pipeline */
                            //ch.pipeline().addLast(new RdmaEncoderTx());
                        }
                    });
            /* general optimization settings */
            boot.option(ChannelOption.SO_BACKLOG, 1024);
            boot.childOption(ChannelOption.SO_KEEPALIVE, true);

            /* now we bind the server and start */
            ChannelFuture f = boot.bind(this.inetSocketAddress.getAddress(),
                    this.inetSocketAddress.getPort()).sync();
            LOG.info("Datanode binded to : " + this.inetSocketAddress);
            /* at this point we are binded and ready */
            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
            acceptGroup.shutdownGracefully();
            LOG.info("Datanode at " + this.inetSocketAddress + " is shutdown");
        }
    }
}

