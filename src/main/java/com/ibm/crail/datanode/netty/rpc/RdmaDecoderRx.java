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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class RdmaDecoderRx extends ByteToMessageDecoder {
    enum DecoderState {
        WAIT_FOR_HEADER, WAIT_FOR_PAYLOAD,
    }

    private RdmaMsgRx rxMsg;
    DecoderState state;

    public RdmaDecoderRx() {
        rxMsg = null;
        state = DecoderState.WAIT_FOR_HEADER;
    }

    protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
        rxMsg = null;
        state = DecoderState.WAIT_FOR_HEADER;
    }

    public void handlerAdded(ChannelHandlerContext ctx) {
        state = DecoderState.WAIT_FOR_HEADER;
    }

    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        switch (state) {
            case WAIT_FOR_HEADER:
                /* in case of init and bytes not ready, we return immediately */
                if (in.readableBytes() < RdmaMsgRx.CSIZE)
                    return;
                /* if enough bytes are around then allocate object and decode the header */
                rxMsg = new RdmaMsgRx();
                rxMsg.decodeHeader(in);
                if (rxMsg.type() == MessageTypes.READ_REQ || rxMsg.type() == MessageTypes.WRITE_RESP) {
                    out.add(rxMsg);
                    state = DecoderState.WAIT_FOR_HEADER;
                    rxMsg = null;
                } else {
                    state = DecoderState.WAIT_FOR_PAYLOAD;
                }
                break;

            case WAIT_FOR_PAYLOAD:
                /* now we check for length when it exceed the expected length */
                if (rxMsg.opLength() > in.readableBytes())
                    return;

                rxMsg.referenceRxPayloadAndRetain(in);
                out.add(rxMsg);
                state = DecoderState.WAIT_FOR_HEADER;
                rxMsg = null;
                break;
        }
    }
}