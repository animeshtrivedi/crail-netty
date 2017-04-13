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
import com.ibm.crail.datanode.netty.rpc.RdmaMsgRx;
import com.ibm.crail.storage.DataResult;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NettyIOResult implements DataResult, Future<DataResult> {
    private int length;
    private boolean finished;
    private ByteBuffer dest;
    private int inError;
    private long cookie;
    private int expectedType;

    public NettyIOResult(){
        length = -1;
        finished = false;
        dest = null;
        inError = -1;
        expectedType = -1;
        cookie = -1;
    }

    public String toString(){
        return " length: " + length +
                " finished " + finished +
                " dest " + dest +
                " inError " + inError +
                " cookie " + cookie +
                " expected Type : "  + MessageTypes.MessageTypeToString(expectedType);
    }

    private void _init(long cookie, int length, ByteBuffer d){
        this.cookie = cookie;
        this.length = length;
        if(d != null) {
            this.dest = d.slice();
        }
    }

    public void initWrite(long cookie, int length){
        _init(cookie, length, null);
        this.expectedType = MessageTypes.WRITE_RESP;
    }
    public void initRead(long cookie, int length, ByteBuffer buf){
        assert (buf != null);
        _init(cookie, length, buf);
        this.expectedType = MessageTypes.READ_RESP;
    }

    public void markDone(RdmaMsgRx finishedMsg){

        assert this.cookie == finishedMsg.cookie();
        assert this.expectedType == finishedMsg.type();
        if(finishedMsg.type() == MessageTypes.READ_RESP) {
            /* this was a read completion, we have to copy out */
            finishedMsg.copyAndReleaseRxPayload(this.dest);
        }

        if(finishedMsg.status() != 0) {
            inError = 1;
        } else {
            inError = 0;
        }

        /* mark us done */
        synchronized (this) {
            finished = true;
            this.notifyAll();
        }
    }

    public int getLen() {
        assert (isDone());
        return this.length;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    public boolean isCancelled() {
        return false;
    }

    public boolean isDone() {
        return finished;
    }

    public DataResult get() throws InterruptedException, ExecutionException {
        synchronized (this) {
            if (!finished) {
                // then we have to block until finished is marked set
                this.wait();
            }
        }
        if(inError != 0)
            throw new InternalError(" The data IO operation has error ");

        if(!finished)
            throw new InterruptedException("Execution interrupted, as the RPC is not finished");

        return this;
    }

    public DataResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        /* otherwise we wait */
        synchronized (this) {
            if (!finished) {
                // then we have to block until finished is marked set
                this.wait(unit.toMillis(timeout));
            }
        }
        if(!finished)
            throw new TimeoutException("Response timeout happened for data transfer");

        if(inError != 0)
            throw new InternalError(" The data IO operation has error ");

        return this;
    }
}