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

package com.ibm.crail.namenode.rpc.netty.common;

import com.ibm.crail.rpc.RpcFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NettyResponseFuture<T> extends NettyCommonFuture implements RpcFuture<T> {
    private T result;
    private String debug;
    private boolean prefetch;
    private boolean done;

    public NettyResponseFuture(String name, T result){
        this.debug = name;
        this.result = result;
        this.done = false;
    }

    final public void markDone() {
        synchronized (this) {
            this.done = true;
            this.notifyAll();
        }
    }

    final public int getTicket() {
        return this.hashCode();
    }

    final public boolean isPrefetched() {
        return this.prefetch;
    }

    final public void setPrefetched(boolean b) {
        this.prefetch = b;
    }

    final public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    final public boolean isCancelled() {
        return false;
    }

    final public boolean isDone() {
        return this.done;
    }

    final public T get() throws InterruptedException, ExecutionException {
        /* otherwise we wait */
        synchronized (this) {
            if (!isDone()) {
                // then we have to block until finished is marked set
                this.wait();
            }
        }
        if(!isDone())
            throw new InterruptedException("RPC was interrupted of kind : "  + debug);

        return result;
    }

    final public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        /* otherwise we wait */
        synchronized (this) {
            if (!isDone()) {
                // then we have to block until finished is marked set
                this.wait(unit.toMillis(timeout));
            }
        }
        if(!isDone())
            throw new TimeoutException("RPC timeout happened for " + debug);

        return result;
    }
}
