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

package com.ibm.crail.datanode.netty.client;

import com.ibm.crail.datanode.DataNodeEndpoint;
import com.ibm.crail.datanode.netty.CrailNettyUtils;
import com.ibm.crail.datanode.netty.rpc.RdmaDecoderRx;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class NettyEndpointGroup {
    private static final Logger LOG = CrailNettyUtils.getLogger();
    private EventLoopGroup workerGroup;
    private Bootstrap boot;
    private ArrayList<NettyEndpoint> activeClients;

    private ConcurrentHashMap<Long, NettyIOResult> inFlightOps;
    private AtomicLong slot;

    public NettyEndpointGroup(){
        workerGroup = new NioEventLoopGroup();
        boot = new Bootstrap();
        boot.group(workerGroup);
        boot.channel(NioSocketChannel.class);
        boot.option(ChannelOption.SO_KEEPALIVE, true);
        final NettyEndpointGroup thisGroup = this;
        boot.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                /* outgoing pipeline */
                //ch.pipeline().addLast(new RdmaEncoderTx());
                /* incoming pipeline */
                ch.pipeline().addLast(new RdmaDecoderRx(), new IncomingResponseHandler(thisGroup));
            }
        });
        activeClients = new ArrayList<NettyEndpoint>();
        slot = new AtomicLong(0);
        inFlightOps = new ConcurrentHashMap<Long, NettyIOResult>();
    }

    final public long getNextSlot(){
        return slot.incrementAndGet();
    }

    final public void insertNewInflight(long slot, NettyIOResult ops){
        inFlightOps.put(slot, ops);
    }

    final public NettyIOResult getAndRemoveInflight(long slot){
        return  inFlightOps.remove(slot);
    }

    public DataNodeEndpoint createEndpoint(final InetSocketAddress inetSocketAddress) throws IOException {
        NettyEndpoint ep = null;
        try {
            /* here we got the client channel and we trigger write on it */
            Channel clientChannel = boot.connect(inetSocketAddress.getAddress(),
                    inetSocketAddress.getPort()).addListener(new GenericFutureListener<Future<? super Void>>() {
                public void operationComplete(Future<? super Void> future) throws Exception {
                    LOG.info("A client->datanode connection established to : " + inetSocketAddress);
                }
            }).sync().channel();
            ep = new NettyEndpoint(this, clientChannel);
            activeClients.add(ep);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return ep;
    }

    public void showCurrentClients(){
        for (NettyEndpoint ep : activeClients) {
            LOG.info(ep.toString());
        }
    }

    public void close() throws InterruptedException, IOException {
        for (NettyEndpoint ep : activeClients) {
            ep.close();
        }
        /* don't care about the future type */
        workerGroup.shutdownGracefully();
    }

    public String toString() {
        return " <TBD> NettyEndpointGroup ! ";
    }

}
