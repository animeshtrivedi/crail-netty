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

import com.ibm.crail.datanode.netty.NettyStorageTier;
import com.ibm.crail.datanode.netty.CrailNettyUtils;
import com.ibm.crail.datanode.netty.rpc.MessageTypes;
import com.ibm.crail.datanode.netty.rpc.RdmaMsgRx;
import com.ibm.crail.datanode.netty.rpc.RdmaMsgTx;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import sun.nio.ch.DirectBuffer;

public class IncomingRequestHandler extends SimpleChannelInboundHandler<RdmaMsgRx> {
    private NettyStorageServer dataNode;
    final Channel channel;
    static private final Logger LOG = CrailNettyUtils.getLogger();

    public IncomingRequestHandler(Channel channel, NettyStorageServer dataNode){
        this.dataNode = dataNode;
        this.channel = channel;
    }

    private void handleRead(ChannelHandlerContext ctx, RdmaMsgRx incomingRead, ByteBuf srcBuf){
        /* for a read, we need to write the buffer */
        int offset = (int) (incomingRead.address() - ((DirectBuffer) srcBuf.nioBuffer()).address());
        final RdmaMsgTx readResponse = incomingRead.makeTxMsg(0, MessageTypes.READ_RESP);
        readResponse.referenceTxPayload(srcBuf, offset, readResponse.opLength());
        //FIXME:
        assert ctx.channel() == this.channel;
        synchronized (this.channel) {
            /* push the header */
            this.channel.write(readResponse.getHeaderPayload());
            /* then data */
            ctx.channel().writeAndFlush(readResponse.getDataPayload().retain()).addListener(new GenericFutureListener<Future<? super Void>>() {
                public void operationComplete(Future<? super Void> future) throws Exception {
                    readResponse.releaseHeaderPayload();
                    readResponse.releaseTxPayload();
                }
            });
        }
    }

    private void handleWrite(ChannelHandlerContext ctx, RdmaMsgRx incomingWrite, ByteBuf targetBuf){
        /* we make the right offset */
        int offset = (int) (incomingWrite.address() - ((DirectBuffer) targetBuf.nioBuffer()).address());
        incomingWrite.copyAndReleaseRxPayload(targetBuf.duplicate(), offset);
        final RdmaMsgTx writeResponse = incomingWrite.makeTxMsg(0, MessageTypes.WRITE_RESP);
        //FIXME:
        assert ctx.channel() == this.channel;
        synchronized (this.channel) {
            /* write the header only, there is no payload */
            this.channel.writeAndFlush(writeResponse.getHeaderPayload()).addListener(new GenericFutureListener<Future<? super Void>>() {
                public void operationComplete(Future<? super Void> future) throws Exception {
                    writeResponse.releaseHeaderPayload();
                }
            });
        }
    }

    protected void channelRead0(ChannelHandlerContext ctx, RdmaMsgRx rxMsg) throws Exception {
        ByteBuf buf = this.dataNode.stagToNettyBuffer(rxMsg.stag());
        if(buf == null) {
            /* then this was a wrong stag, send back reply */
            CrailNettyUtils.getLogger().error("Rejecting stag for " + rxMsg);
            RdmaMsgTx txMsg;
            if(rxMsg.type() == MessageTypes.READ_REQ){
                txMsg = rxMsg.makeTxMsg(-1, MessageTypes.READ_RESP);
            } else {
                txMsg = rxMsg.makeTxMsg(-1, MessageTypes.WRITE_RESP);
            }
            ctx.channel().writeAndFlush(txMsg);
            return;
        }
        /* we make duplicate as there can be multiple reqs concurrently on the buffer */
        if(rxMsg.type() == MessageTypes.READ_REQ){
            handleRead(ctx, rxMsg, buf);
        } else {
            handleWrite(ctx, rxMsg, buf);
        }
    }

    @Override
    final public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
