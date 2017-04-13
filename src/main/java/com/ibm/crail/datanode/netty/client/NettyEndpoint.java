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

import com.ibm.crail.datanode.netty.rpc.MessageTypes;
import com.ibm.crail.datanode.netty.rpc.RdmaMsgTx;
import com.ibm.crail.metadata.BlockInfo;

import com.ibm.crail.storage.DataResult;
import com.ibm.crail.storage.StorageEndpoint;
import io.netty.channel.*;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

public class NettyEndpoint implements StorageEndpoint {
    private Channel noAtomicClientChannel;
    private NettyEndpointGroup group;

    public NettyEndpoint(NettyEndpointGroup group, Channel c){
        this.noAtomicClientChannel = c;
        this.group = group;
    }

    private ChannelFuture atomicFlush(RdmaMsgTx tx, boolean isRead){
        ChannelFuture ftx;
        synchronized (this){
            if(isRead) {
                ftx = noAtomicClientChannel.writeAndFlush(tx.getHeaderPayload());
            } else {
                noAtomicClientChannel.write(tx.getHeaderPayload());
                ftx = noAtomicClientChannel.writeAndFlush(tx.getDataPayload().retain());
            }
        }
        return ftx;
    }

    public Future<DataResult> write(ByteBuffer wBuffer, ByteBuffer region, BlockInfo remoteMr, long remoteOffset) throws IOException, InterruptedException{
        final RdmaMsgTx tx = new RdmaMsgTx();
        long id = this.group.getNextSlot();
        NettyIOResult w = new NettyIOResult();
        int len = wBuffer.remaining();
        w.initWrite(id, len);
        this.group.insertNewInflight(id, w);

        tx.initHeader(remoteMr.getAddr() + remoteOffset,
                len,
                remoteMr.getLkey(),
                MessageTypes.WRITE_REQ,
                0,
                id);
        tx.referenceTxPayload(wBuffer);
        this.atomicFlush(tx, false).addListener(
                new GenericFutureListener<io.netty.util.concurrent.Future<? super Void>>() {
            public void operationComplete(io.netty.util.concurrent.Future<? super Void> future) throws Exception {
                assert (!MessageTypes.isTypeIllegal(tx.type()));
                tx.releaseHeaderPayload();
                tx.releaseTxPayload();
            }
        });
        return w;
    }

    public Future<DataResult> read(ByteBuffer rBuffer, ByteBuffer region, BlockInfo remoteMr, long remoteOffset) throws IOException, InterruptedException{
        RdmaMsgTx tx = new RdmaMsgTx();
        long id = this.group.getNextSlot();
        NettyIOResult r = new NettyIOResult();
        int len = rBuffer.limit() - rBuffer.position();
        r.initRead(id, len, rBuffer);
        this.group.insertNewInflight(id, r);
        tx.initHeader(remoteMr.getAddr() + remoteOffset,
                len,
                remoteMr.getLkey(),
                MessageTypes.READ_REQ,
                0,
                id);

        this.atomicFlush(tx, true);
        return r;
    }

    public void close() throws IOException, InterruptedException{
        /* don't care about the sync */
        synchronized (this) {
            noAtomicClientChannel.close();
        }
    }

    public boolean isLocal(){
        return false;
    }

    public String toString(){
        return this.noAtomicClientChannel.toString();
    }
}
