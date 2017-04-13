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

import com.ibm.crail.datanode.netty.CrailNettyUtils;
import com.ibm.crail.namenode.rpc.netty.client.NettyRPCNamenodeClientGroup;
import com.ibm.crail.namenode.rpc.netty.common.*;
import com.ibm.crail.rpc.RpcBinding;
import com.ibm.crail.rpc.RpcConnection;
import com.ibm.crail.rpc.RpcNameNodeService;
import com.ibm.crail.utils.CrailUtils;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.slf4j.Logger;

import java.net.InetSocketAddress;

public class NettyNameNode implements RpcBinding {
    static private Logger LOG = CrailNettyUtils.getLogger();
    private NettyRPCNamenodeClientGroup clientGroup;

    public NettyNameNode(){
        clientGroup = null;
    }

    /* This is inherited from the RPCBinding - the passed service object is where the RPCs are processed. */
    public void run(final RpcNameNodeService service){
        /* here we run the incoming RPC service */
        InetSocketAddress inetSocketAddress = CrailUtils.getNameNodeAddress();
        LOG.info("Starting the NettyNamenode service at : " + inetSocketAddress);
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
                    LOG.info("A new connection has arrived from : " + ch.remoteAddress().toString());
                            /* incoming pipeline */
                    ch.pipeline().addLast("RequestDecoder" , new RequestDecoder());
                    ch.pipeline().addLast("NNProcessor", new NamenodeProcessor(service));
                            /* outgoing pipeline */
                    ch.pipeline().addLast("ResponseEncoder", new ResponseEncoder());
                }
            });
            /* general optimization settings */
            boot.option(ChannelOption.SO_BACKLOG, 1024);
            boot.childOption(ChannelOption.SO_KEEPALIVE, true);

            /* now we bind the server and start */
            ChannelFuture f = boot.bind(inetSocketAddress.getAddress(),
                    inetSocketAddress.getPort()).sync();
            /* at this point we are binded and ready */
            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
            acceptGroup.shutdownGracefully();
            LOG.info("Netty namenode at " + inetSocketAddress + " is shutdown");
        }
    }

    /* This function comes from RPCClient interface */
    public RpcConnection connect(InetSocketAddress address) {
        if(clientGroup == null) {
            /* this should be the client side code */
            clientGroup = new NettyRPCNamenodeClientGroup();
        }
        return clientGroup.getClient(address);
    }

    /* This function comes from RPCClient interface */
    public void close(){
        if(this.clientGroup != null) {
            /* after this close rest of the infrastructure */
            this.clientGroup.closeClientGroup();
        }
    }
}
