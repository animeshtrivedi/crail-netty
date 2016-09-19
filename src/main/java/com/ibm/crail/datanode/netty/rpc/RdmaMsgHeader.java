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

package com.ibm.crail.datanode.netty.rpc;

import io.netty.buffer.ByteBuf;

public class RdmaMsgHeader {

    protected long address; // 8
    protected int opLength; // +4
    protected int stag; // +4
    protected int type; // +4
    protected int status; // +4
    protected long cookie; // +8 = 32;

    public static int CSIZE = 32;

    public RdmaMsgHeader(){
        address = -1;
        opLength = -1;
        type = -1;
        status = -1;
        cookie = -1;
        stag = -1;
    }

    final public void initHeader(long addr, int len, int stag, int type, int status, long cookie){
        this.address = addr;
        this.opLength = len;
        this.stag = stag;
        this.type = type;
        this.status = status;
        this.cookie = cookie;
    }

    final public long cookie(){
        return cookie;
    }

    public void encode(ByteBuf target){
        target.writeLong(address);
        target.writeInt(opLength);
        target.writeInt(stag);
        target.writeInt(type);
        target.writeInt(status);
        target.writeLong(cookie);
    }

    public void decodeHeader(ByteBuf src){
        this.address = src.readLong();
        this.opLength= src.readInt();
        this.stag = src.readInt();
        this.type = src.readInt();
        this.status = src.readInt();
        this.cookie = src.readLong();
    }

    final public long address(){
        return address;
    }

    final public int stag(){
        return stag;
    }

    final public int opLength(){
        return opLength;
    }

    final public int status(){
        return status;
    }

    final public int type(){
        return type;
    }

    public String toString() {
        return "RDMAMsg addr: 0x" + Long.toHexString(address) + " len: " + opLength + " stag : " + stag + " type: " + MessageTypes.MessageTypeToString(type) + " status: " + status + " cookie: " + cookie;
    }
}

