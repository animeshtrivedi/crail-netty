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

package com.ibm.crail.namenode.rpc.netty;

import com.ibm.crail.datanode.netty.CrailNettyUtils;
import com.ibm.crail.namenode.rpc.netty.common.NettyRequest;
import com.ibm.crail.namenode.rpc.netty.common.NettyResponse;

import com.ibm.crail.rpc.RpcNameNodeService;
import com.ibm.crail.rpc.RpcProtocol;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;

public class NamenodeProcessor extends SimpleChannelInboundHandler<NettyRequest> {
    static private Logger LOG = CrailNettyUtils.getLogger();
    RpcNameNodeService service;

    public NamenodeProcessor(RpcNameNodeService service) {
        this.service = service;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyRequest request) throws Exception {
        NettyResponse response = new NettyResponse();
        short error;
        try {
            response.setTypeAndAllocate(RpcProtocol.responseTypes[request.getCmd()], request.getCookie());
            switch(request.getCmd()) {
                case RpcProtocol.CMD_CREATE_FILE:
                    error = service.createFile(request.createFile(), response.createFile(), response);
                    break;
                case RpcProtocol.CMD_GET_FILE:
                    error = service.getFile(request.getFile(), response.getFile(), response);
                    break;
                case RpcProtocol.CMD_SET_FILE:
                    error = service.setFile(request.setFile(), response.getVoid(), response);
                    break;
                case RpcProtocol.CMD_REMOVE_FILE:
                    error = service.removeFile(request.removeFile(), response.delFile(), response);
                    break;
                case RpcProtocol.CMD_RENAME_FILE:
                    error = service.renameFile(request.renameFile(), response.getRename(), response);
                    break;
                case RpcProtocol.CMD_GET_BLOCK:
                    error = service.getBlock(request.getBlock(), response.getBlock(), response);
                    break;
                case RpcProtocol.CMD_GET_LOCATION:
                    error = service.getLocation(request.getLocation(), response.getLocation(), response);
                    break;
                case RpcProtocol.CMD_SET_BLOCK:
                    error = service.setBlock(request.setBlock(), response.getVoid(), response);
                    break;
                case RpcProtocol.CMD_GET_DATANODE:
                    error = service.getDataNode(request.getDataNode(), response.getDataNode(), response);
                    break;
                case RpcProtocol.CMD_DUMP_NAMENODE:
                    error = service.dump(request.dumpNameNode(), response.getVoid(), response);
                    break;
                case RpcProtocol.CMD_PING_NAMENODE:
                    error = service.ping(request.pingNameNode(), response.pingNameNode(), response);
                    break;
                default:
                    error = RpcProtocol.ERR_INVALID_RPC_CMD;
                    LOG.info("Rpc command not valid, opcode " + request.getCmd());
            }
        } catch(Exception e){
            error = RpcProtocol.ERR_UNKNOWN;
            LOG.info(RpcProtocol.messages[RpcProtocol.ERR_UNKNOWN] + e.getMessage());
            e.printStackTrace();
        }

        try {
            /* set error code and flush */
            response.setError(error);
            ctx.channel().writeAndFlush(response);
        } catch(Exception e){
            LOG.info("ERROR: RPC failed, messagesSend ");
            e.printStackTrace();
        }
    }
}
