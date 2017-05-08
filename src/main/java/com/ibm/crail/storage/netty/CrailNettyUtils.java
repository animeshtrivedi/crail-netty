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

package com.ibm.crail.storage.netty;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by atr on 09.08.16.
 */
public class CrailNettyUtils {
    private static Logger LOG = getLogger();
    private CrailNettyUtils(){}
    public static synchronized Logger getLogger() {
        if(LOG == null) {
            LOG = LoggerFactory.getLogger("com.ibm.crail.storage.netty");
        }
        return LOG;
    }

    public static void showByteBufferContent(ByteBuffer buf, int offset, int bytes){
        /* this dump the content of first bytes from the payload */
        if(buf != null){
            LOG.info("DUMP: TID:" + Thread.currentThread().getId() + " NioByteBuffer : " + buf);
            int min = (buf.limit() - offset);
            if(min > bytes)
                min = bytes;
            String str = "DUMP: TID:" + Thread.currentThread().getId() + " DUMP (" + offset+" ,+" + min + ") : ";
            //for loop update it to the end limit
            min+=offset;
            for(int i = offset; i < min; i++){
                //str += Character.toHexString();
                str += Byte.toString(buf.get(i)) + " : " ;
                if(i%32 == 0)
                    str+="\n";
            }
            LOG.info(str);
        } else {
            LOG.info("DUMP : payload content is NULL");
        }
    }

    public static void showByteBufContent(ByteBuf buf, int offset, int bytes){
        /* this dump the content of first bytes from the payload */
        if(buf != null){
            int ori_rindex = buf.readerIndex();
            LOG.info("DUMP: TID:" + Thread.currentThread().getId() + " NettyByteBuf : " + buf);
            int min = (buf.capacity() - offset);
            if(min > bytes)
                min = bytes;
            String str = "DUMP: TID:" + Thread.currentThread().getId() + " DUMP (" + offset+" ,+" + min + ") : ";
            //for loop update it to the end limit
            min+=offset;
            for(int i = offset; i < min; i++){
                //str += Character.toHexString();
                str += Byte.toString(buf.getByte(i)) + " : " ;
                if(i%32 == 0)
                    str+="\n";
            }
            LOG.info(str);
            buf.readerIndex(ori_rindex);
        } else {
            LOG.info("DUMP : payload content is NULL");
        }
    }
}
