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

package com.ibm.crail.namenode.rpc.netty.client;

import com.ibm.crail.namenode.rpc.netty.common.NettyResponse;
import com.ibm.crail.namenode.rpc.netty.common.RequestEncoder;
import com.ibm.crail.namenode.rpc.netty.common.ResponseDecoder;
import com.ibm.crail.storage.netty.CrailNettyUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class NettyRPCNamenodeClientGroup {
    private static final Logger LOG = CrailNettyUtils.getLogger();
    private EventLoopGroup workerGroup;
    private Bootstrap boot;

    private ConcurrentHashMap<Long, NettyResponse> inFlightOps;
    private AtomicLong slot;

    private ArrayList<NettyRPCNamenodeClient> activeClients;

    public NettyRPCNamenodeClientGroup(){
        workerGroup = new NioEventLoopGroup();
        boot = new Bootstrap();
        boot.group(workerGroup);
        boot.channel(NioSocketChannel.class);
        boot.option(ChannelOption.SO_KEEPALIVE, true);
        final NettyRPCNamenodeClientGroup thisGroup = this;
        boot.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                /* outgoing pipeline */
                ch.pipeline().addLast(new RequestEncoder());
                /* incoming pipeline */
                ch.pipeline().addLast(new ResponseDecoder(thisGroup));
            }
        });

        slot = new AtomicLong(1);
        inFlightOps = new ConcurrentHashMap<Long, NettyResponse>();
        activeClients = new ArrayList<NettyRPCNamenodeClient>();
    }

    public void closeClientGroup(){
        /* check in flight */
        if(inFlightOps.size() != 0) {
            LOG.error("There are in flight requests");
            Thread.dumpStack();
        }
        for (NettyRPCNamenodeClient c : activeClients) {
            c.close();
        }
        activeClients.clear();
        try {
            workerGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public long getNextSlot(){
        return slot.incrementAndGet();
    }

    public void insertNewInflight(long slot, NettyResponse ops){
        inFlightOps.put(slot, ops);
    }

    public NettyResponse retriveAndRemove(long slot){
        return inFlightOps.remove(slot);
    }

    public NettyRPCNamenodeClient getClient(InetSocketAddress inetSocketAddress) {
        Channel clientChannel = null;
        try {
            clientChannel = boot.connect(inetSocketAddress.getAddress(),
                inetSocketAddress.getPort()).sync().channel();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("Connected to the Netty Namenode at : " + inetSocketAddress);
        NettyRPCNamenodeClient ep = new NettyRPCNamenodeClient(clientChannel, this);
        activeClients.add(ep);
        return ep;
    }
}
