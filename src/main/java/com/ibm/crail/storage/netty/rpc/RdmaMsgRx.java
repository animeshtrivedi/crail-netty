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

public class RdmaMsgRx extends RdmaMsgHeader {
    ByteBuf rxPayload;

    public RdmaMsgRx(){
        this.rxPayload = null;
    }

    final public void referenceRxPayloadAndRetain(ByteBuf src){
        this.rxPayload = src.readSlice(this.opLength).retain();
        assert this.rxPayload.readableBytes() == this.opLength;
    }

    final public void copyAndReleaseRxPayload(ByteBuffer dst){
        this.rxPayload.readBytes(dst);
        this.rxPayload.release();
    }

    final public void copyAndReleaseRxPayload(ByteBuf dst, int index){
        this.rxPayload.readBytes(dst, index, this.opLength);
        assert this.rxPayload.readableBytes() == 0;
        this.rxPayload.release();
    }

    final public String toString() {
        return super.toString() + " rxPayload: " + (this.rxPayload == null?" NULL ":" NotNULL ");
    }

    final public RdmaMsgTx makeTxMsg(int ecode, int type){
        RdmaMsgTx msg = new RdmaMsgTx();
        /* we copy the message as it is - with the given ecode and type */
        msg.initHeader(this.address, this.opLength, this.stag, type, ecode, this.cookie);
        return  msg;
    }
}
