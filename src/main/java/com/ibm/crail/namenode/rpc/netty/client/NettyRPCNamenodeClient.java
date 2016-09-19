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

package com.ibm.crail.namenode.rpc.netty.client;

import com.ibm.crail.datanode.netty.CrailNettyUtils;
import com.ibm.crail.namenode.rpc.netty.common.NettyRequest;
import com.ibm.crail.namenode.rpc.netty.common.NettyResponse;
import com.ibm.crail.namenode.rpc.netty.common.NettyResponseFuture;
import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.namenode.protocol.DataNodeInfo;
import com.ibm.crail.namenode.protocol.FileInfo;
import com.ibm.crail.namenode.protocol.FileName;
import com.ibm.crail.namenode.rpc.*;

import io.netty.channel.Channel;

import org.slf4j.Logger;

import java.io.IOException;

public class NettyRPCNamenodeClient implements RpcNameNodeClient {
    static private final Logger LOG = CrailNettyUtils.getLogger();
    private Channel clientChannel;
    private NettyRPCNamenodeClientGroup group;

    public NettyRPCNamenodeClient(Channel clientChannel, NettyRPCNamenodeClientGroup grp){
        this.clientChannel = clientChannel;
        this.group = grp;
    }

    public String toString(){
        return this.clientChannel.toString();
    }

    public void close(){
        /* don't care about the future */
        this.clientChannel.close();
    }

    public RpcNameNodeFuture<RpcResponseMessage.CreateFileRes> createFile(FileName fileName, boolean isDir, int storageAffinity, int locationAffinity)
            throws IOException {
        long cookie = this.group.getNextSlot();
        /* get a new request that will travel on wire */
        RpcRequestMessage.CreateFileReq req = new RpcRequestMessage.CreateFileReq(fileName,
                isDir, storageAffinity, locationAffinity);
        /* get a response back that will be serialized */
        RpcResponseMessage.CreateFileRes resp = new RpcResponseMessage.CreateFileRes();
        /* construct a response */
        NettyResponseFuture<RpcResponseMessage.CreateFileRes> resultF =
                new NettyResponseFuture<RpcResponseMessage.CreateFileRes>("createFile",
                        resp);
        /* respF goes into the map */
        this.group.insertNewInflight(cookie, new NettyResponse(resp, cookie, resultF));
        /* now we construct and push out the request */
        this.clientChannel.writeAndFlush(new NettyRequest(req, cookie));
        return resultF;
    }

    public RpcNameNodeFuture<RpcResponseMessage.GetFileRes> getFile(FileName fileName, boolean b) throws IOException {
        long cookie = this.group.getNextSlot();
        /* get a new request that will travel on wire */
        RpcRequestMessage.GetFileReq req = new RpcRequestMessage.GetFileReq(fileName, b);
        /* get a response back that will be serialized */
        RpcResponseMessage.GetFileRes resp = new RpcResponseMessage.GetFileRes();
        /* construct a response */
        NettyResponseFuture<RpcResponseMessage.GetFileRes> resultF =
                new NettyResponseFuture<RpcResponseMessage.GetFileRes>("getFile",
                        resp);
        /* respF goes into the map */
        this.group.insertNewInflight(cookie, new NettyResponse(resp, cookie, resultF));
        /* now we construct and push out the request */
        this.clientChannel.writeAndFlush(new NettyRequest(req, cookie));
        return resultF;
    }

    public RpcNameNodeFuture<RpcResponseMessage.VoidRes> setFile(FileInfo fileInfo, boolean b) throws IOException {
        long cookie = this.group.getNextSlot();
        /* get a new request that will travel on wire */
        RpcRequestMessage.SetFileReq req = new RpcRequestMessage.SetFileReq(fileInfo, b);
        /* get a response back that will be serialized */
        RpcResponseMessage.VoidRes resp = new RpcResponseMessage.VoidRes();
        /* construct a response */
        NettyResponseFuture<RpcResponseMessage.VoidRes> resultF =
                new NettyResponseFuture<RpcResponseMessage.VoidRes>("setFile",
                        resp);
        /* respF goes into the map */
        this.group.insertNewInflight(cookie, new NettyResponse(resp, cookie, resultF));
        /* now we construct and push out the request */
        this.clientChannel.writeAndFlush(new NettyRequest(req, cookie));
        return resultF;
    }

    public RpcNameNodeFuture<RpcResponseMessage.DeleteFileRes> removeFile(FileName fileName, boolean b) throws IOException {
        long cookie = this.group.getNextSlot();
        /* get a new request that will travel on wire */
        RpcRequestMessage.RemoveFileReq req = new RpcRequestMessage.RemoveFileReq(fileName, b);
        /* get a response back that will be serialized */
        RpcResponseMessage.DeleteFileRes resp = new RpcResponseMessage.DeleteFileRes();
        /* construct a response */
        NettyResponseFuture<RpcResponseMessage.DeleteFileRes> resultF =
                new NettyResponseFuture<RpcResponseMessage.DeleteFileRes>("deleteFile",
                        resp);
        /* respF goes into the map */
        this.group.insertNewInflight(cookie, new NettyResponse(resp, cookie, resultF));
        /* now we construct and push out the request */
        this.clientChannel.writeAndFlush(new NettyRequest(req, cookie));
        return resultF;
    }

    public RpcNameNodeFuture<RpcResponseMessage.RenameRes> renameFile(FileName fileName, FileName fileName1) throws IOException {
        long cookie = this.group.getNextSlot();
        /* get a new request that will travel on wire */
        RpcRequestMessage.RenameFileReq req = new RpcRequestMessage.RenameFileReq(fileName, fileName1);
        /* get a response back that will be serialized */
        RpcResponseMessage.RenameRes resp = new RpcResponseMessage.RenameRes();
        /* construct a response */
        NettyResponseFuture<RpcResponseMessage.RenameRes> resultF =
                new NettyResponseFuture<RpcResponseMessage.RenameRes>("renameFile",
                        resp);
        /* respF goes into the map */
        this.group.insertNewInflight(cookie, new NettyResponse(resp, cookie, resultF));
        /* now we construct and push out the request */
        this.clientChannel.writeAndFlush(new NettyRequest(req, cookie));
        return resultF;
    }

    public RpcNameNodeFuture<RpcResponseMessage.GetBlockRes> getBlock(long fd, long token, long position,
                                                                      int storageAffinity, int locationAffinity,
                                                                      long capacity) throws IOException {
        long cookie = this.group.getNextSlot();
        /* get a new request that will travel on wire */
        RpcRequestMessage.GetBlockReq req = new RpcRequestMessage.GetBlockReq(fd, token, position,
                storageAffinity, locationAffinity, capacity);
        /* get a response back that will be serialized */
        RpcResponseMessage.GetBlockRes resp = new RpcResponseMessage.GetBlockRes();
        /* construct a response */
        NettyResponseFuture<RpcResponseMessage.GetBlockRes> resultF =
                new NettyResponseFuture<RpcResponseMessage.GetBlockRes>("getBlock",
                        resp);
        /* respF goes into the map */
        this.group.insertNewInflight(cookie, new NettyResponse(resp, cookie, resultF));
        /* now we construct and push out the request */
        this.clientChannel.writeAndFlush(new NettyRequest(req, cookie));
        return resultF;
    }

    public RpcNameNodeFuture<RpcResponseMessage.GetLocationRes> getLocation(FileName fileName, long l) throws IOException {
        long cookie = this.group.getNextSlot();
        /* get a new request that will travel on wire */
        RpcRequestMessage.GetLocationReq req = new RpcRequestMessage.GetLocationReq(fileName, l);
        /* get a response back that will be serialized */
        RpcResponseMessage.GetLocationRes resp = new RpcResponseMessage.GetLocationRes();
        /* construct a response */
        NettyResponseFuture<RpcResponseMessage.GetLocationRes> resultF =
                new NettyResponseFuture<RpcResponseMessage.GetLocationRes>("getLocation",
                        resp);
        /* respF goes into the map */
        this.group.insertNewInflight(cookie, new NettyResponse(resp, cookie, resultF));
        /* now we construct and push out the request */
        this.clientChannel.writeAndFlush(new NettyRequest(req, cookie));
        return resultF;
    }

    public RpcNameNodeFuture<RpcResponseMessage.VoidRes> setBlock(BlockInfo blockInfo) throws Exception {
        long cookie = this.group.getNextSlot();
        /* get a new request that will travel on wire */
        RpcRequestMessage.SetBlockReq req = new RpcRequestMessage.SetBlockReq(blockInfo);
        /* get a response back that will be serialized */
        RpcResponseMessage.VoidRes resp = new RpcResponseMessage.VoidRes();
        /* construct a response */
        NettyResponseFuture<RpcResponseMessage.VoidRes> resultF =
                new NettyResponseFuture<RpcResponseMessage.VoidRes>("setBlock",
                        resp);
        /* respF goes into the map */
        this.group.insertNewInflight(cookie, new NettyResponse(resp, cookie, resultF));
        /* now we construct and push out the request */
        this.clientChannel.writeAndFlush(new NettyRequest(req, cookie));
        return resultF;
    }

    public RpcNameNodeFuture<RpcResponseMessage.GetDataNodeRes> getDataNode(DataNodeInfo dataNodeInfo) throws Exception {
        long cookie = this.group.getNextSlot();
        /* get a new request that will travel on wire */
        RpcRequestMessage.GetDataNodeReq req = new RpcRequestMessage.GetDataNodeReq(dataNodeInfo);
        /* get a response back that will be serialized */
        RpcResponseMessage.GetDataNodeRes resp = new RpcResponseMessage.GetDataNodeRes();
        /* construct a response */
        NettyResponseFuture<RpcResponseMessage.GetDataNodeRes> resultF =
                new NettyResponseFuture<RpcResponseMessage.GetDataNodeRes>("getDataNode",
                        resp);
        /* goes into the map */
        this.group.insertNewInflight(cookie, new NettyResponse(resp, cookie, resultF));
        /* now we construct and push out the request */
        this.clientChannel.writeAndFlush(new NettyRequest(req, cookie));
        return resultF;
    }

    public RpcNameNodeFuture<RpcResponseMessage.VoidRes> dumpNameNode() throws Exception {
        long cookie = this.group.getNextSlot();
        /* get a new request that will travel on wire */
        RpcRequestMessage.DumpNameNodeReq req = new RpcRequestMessage.DumpNameNodeReq();
        /* get a response back that will be serialized */
        RpcResponseMessage.VoidRes resp = new RpcResponseMessage.VoidRes();
        /* construct a response */
        NettyResponseFuture<RpcResponseMessage.VoidRes> resultF =
                new NettyResponseFuture<RpcResponseMessage.VoidRes>("dumpNameNode",
                        resp);
        /* respF goes into the map */
        this.group.insertNewInflight(cookie, new NettyResponse(resp, cookie, resultF));
        /* now we construct and push out the request */
        this.clientChannel.writeAndFlush(new NettyRequest(req, cookie));
        return resultF;
    }

    public RpcNameNodeFuture<RpcResponseMessage.PingNameNodeRes> pingNameNode() throws Exception {
        long cookie = this.group.getNextSlot();
        /* get a new request that will travel on wire */
        RpcRequestMessage.PingNameNodeReq req = new RpcRequestMessage.PingNameNodeReq();
        /* get a response back that will be serialized */
        RpcResponseMessage.PingNameNodeRes resp = new RpcResponseMessage.PingNameNodeRes();
        /* construct a response */
        NettyResponseFuture<RpcResponseMessage.PingNameNodeRes> resultF =
                new NettyResponseFuture<RpcResponseMessage.PingNameNodeRes>("pingNameNode",
                        resp);
        /* respF goes into the map */
        this.group.insertNewInflight(cookie, new NettyResponse(resp, cookie, resultF));
        /* now we construct and push out the request */
        this.clientChannel.writeAndFlush(new NettyRequest(req, cookie));
        return resultF;
    }
}
