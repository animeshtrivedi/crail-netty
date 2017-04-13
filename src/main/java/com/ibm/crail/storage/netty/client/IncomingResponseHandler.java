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

package com.ibm.crail.storage.netty.client;

import com.ibm.crail.storage.netty.rpc.RdmaMsgRx;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class IncomingResponseHandler extends SimpleChannelInboundHandler<RdmaMsgRx> {
    private NettyEndpointGroup group;

    public IncomingResponseHandler(NettyEndpointGroup grp){
        this.group = grp;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RdmaMsgRx rxMsg) throws Exception {
        long cookie = rxMsg.cookie();
        NettyIOResult result = this.group.getAndRemoveInflight(cookie);
        assert result!=null;
        result.markDone(rxMsg);
    }

    @Override
    final public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}

