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

package com.ibm.crail.storage.netty.rpc;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

import static io.netty.buffer.Unpooled.directBuffer;
import static io.netty.buffer.Unpooled.wrappedBuffer;

public class RdmaMsgTx extends RdmaMsgHeader {
    ByteBuf txPayload;
    ByteBuf header;
    boolean encoded;

    public RdmaMsgTx(){
        header = directBuffer(RdmaMsgHeader.CSIZE, RdmaMsgHeader.CSIZE).retain();
        txPayload = null;
        encoded = false;
    }

    public void referenceTxPayload(ByteBuffer src){
        this.txPayload = wrappedBuffer(src).retain();
    }

    public void referenceTxPayload(ByteBuf src, int position, int length){
        this.txPayload = src.slice(position, length).retain();
    }

    public void releaseTxPayload(){
        this.txPayload.release();
        this.txPayload = null;
    }

    public void releaseHeaderPayload(){
        header.release();
        this.header = null;
    }

    public ByteBuf getHeaderPayload(){
        if(!encoded) {
            header.clear();
            super.encode(header);
            encoded = true;
        }
        return header;
    }

    public ByteBuf getDataPayload(){
        assert (this.txPayload.readableBytes() == opLength);
        return txPayload;
    }

    public String toString() {
        return super.toString() + " txPayload: " + (this.txPayload == null?" NULL ":" NotNULL ");
    }
}
