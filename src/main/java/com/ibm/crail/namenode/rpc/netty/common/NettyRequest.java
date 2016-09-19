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

import com.ibm.crail.namenode.rpc.NameNodeProtocol;
import com.ibm.crail.namenode.rpc.RpcRequestMessage;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NettyRequest {
    public static final int CSIZE = 12 + Math.max(RpcRequestMessage.SetFileReq.CSIZE, RpcRequestMessage.RenameFileReq.CSIZE);

    private short cmd;
    private short type;
    private long cookie;

    private RpcRequestMessage.CreateFileReq createFileReq;
    private RpcRequestMessage.GetFileReq fileReq;
    private RpcRequestMessage.SetFileReq setFileReq;
    private RpcRequestMessage.RemoveFileReq removeReq;
    private RpcRequestMessage.RenameFileReq renameFileReq;
    private RpcRequestMessage.GetBlockReq getBlockReq;
    private RpcRequestMessage.GetLocationReq getLocationReq;
    private RpcRequestMessage.SetBlockReq setBlockReq;
    private RpcRequestMessage.GetDataNodeReq getDataNodeReq;
    private RpcRequestMessage.DumpNameNodeReq dumpNameNodeReq;
    private RpcRequestMessage.PingNameNodeReq pingNameNodeReq;

    private ByteBuffer nioBuffer;

    public NettyRequest() {
        this.cmd = 0;
        this.type = 0;
        this.cookie = 0;
        /* -12 because cmd, type, cookie are not put through the byte buffer */
        this.nioBuffer = ByteBuffer.allocateDirect(NettyRequest.CSIZE - 12);
    }

    public NettyRequest(RpcRequestMessage.CreateFileReq message, long cookie) {
        this();
        this.cookie = cookie;
        this.type = message.getType();
        this.createFileReq = message;
        this.cmd = NameNodeProtocol.CMD_CREATE_FILE;
    }

    public NettyRequest(RpcRequestMessage.GetFileReq message, long cookie) {
        this();
        this.cookie = cookie;
        this.type = message.getType();
        this.fileReq = message;
        this.cmd = NameNodeProtocol.CMD_GET_FILE;
    }

    public NettyRequest(RpcRequestMessage.SetFileReq message, long cookie) {
        this();
        this.cookie = cookie;
        this.type = message.getType();
        this.setFileReq = message;
        this.cmd = NameNodeProtocol.CMD_SET_FILE;
    }

    public NettyRequest(RpcRequestMessage.RemoveFileReq message, long cookie) {
        this();
        this.cookie = cookie;
        this.type = message.getType();
        this.removeReq = message;
        this.cmd = NameNodeProtocol.CMD_REMOVE_FILE;
    }

    public NettyRequest(RpcRequestMessage.RenameFileReq message, long cookie) {
        this();
        this.cookie = cookie;
        this.type = message.getType();
        this.renameFileReq = message;
        this.cmd = NameNodeProtocol.CMD_RENAME_FILE;
    }

    public NettyRequest(RpcRequestMessage.GetBlockReq message, long cookie) {
        this();
        this.cookie = cookie;
        this.type = message.getType();
        this.getBlockReq = message;
        this.cmd = NameNodeProtocol.CMD_GET_BLOCK;
    }

    public NettyRequest(RpcRequestMessage.GetLocationReq message, long cookie) {
        this();
        this.cookie = cookie;
        this.type = message.getType();
        this.getLocationReq = message;
        this.cmd = NameNodeProtocol.CMD_GET_LOCATION;
    }

    public NettyRequest(RpcRequestMessage.SetBlockReq message, long cookie) {
        this();
        this.cookie = cookie;
        this.type = message.getType();
        this.setBlockReq = message;
        this.cmd = NameNodeProtocol.CMD_SET_BLOCK;
    }

    public NettyRequest(RpcRequestMessage.GetDataNodeReq message, long cookie) {
        this();
        this.cookie = cookie;
        this.type = message.getType();
        this.getDataNodeReq = message;
        this.cmd = NameNodeProtocol.CMD_GET_DATANODE;
    }

    public NettyRequest(RpcRequestMessage.DumpNameNodeReq message, long cookie) {
        this();
        this.cookie = cookie;
        this.type = message.getType();
        this.dumpNameNodeReq = message;
        this.cmd = NameNodeProtocol.CMD_DUMP_NAMENODE;
    }

    public NettyRequest(RpcRequestMessage.PingNameNodeReq message, long cookie) {
        this();
        this.cookie = cookie;
        this.type = message.getType();
        this.pingNameNodeReq = message;
        this.cmd = NameNodeProtocol.CMD_PING_NAMENODE;
    }

    public long getCookie(){
        return cookie;
    }

    public int write(ByteBuf buffer) throws IOException {
        buffer.writeLong(cookie); //8
        buffer.writeShort(cmd); //2
        buffer.writeShort(type); //2
        int written = 12;

        nioBuffer.clear();
        switch (type) {
            case NameNodeProtocol.REQ_CREATE_FILE:
                written += createFileReq.write(nioBuffer);
                break;
            case NameNodeProtocol.REQ_GET_FILE:
                written += fileReq.write(nioBuffer);
                break;
            case NameNodeProtocol.REQ_SET_FILE:
                written += setFileReq.write(nioBuffer);
                break;
            case NameNodeProtocol.REQ_REMOVE_FILE:
                written += removeReq.write(nioBuffer);
                break;
            case NameNodeProtocol.REQ_RENAME_FILE:
                written += renameFileReq.write(nioBuffer);
                break;
            case NameNodeProtocol.REQ_GET_BLOCK:
                written += getBlockReq.write(nioBuffer);
                break;
            case NameNodeProtocol.REQ_GET_LOCATION:
                written += getLocationReq.write(nioBuffer);
                break;
            case NameNodeProtocol.REQ_SET_BLOCK:
                written += setBlockReq.write(nioBuffer);
                break;
            case NameNodeProtocol.REQ_GET_DATANODE:
                written += getDataNodeReq.write(nioBuffer);
                break;
            case NameNodeProtocol.REQ_DUMP_NAMENODE:
                written += dumpNameNodeReq.write(nioBuffer);
                break;
            case NameNodeProtocol.REQ_PING_NAMENODE:
                written += pingNameNodeReq.write(nioBuffer);
                break;
        }
        /* instead of flip you want to clear it */
        nioBuffer.clear();
        buffer.writeBytes(nioBuffer);
        return written;
    }

    public void update(ByteBuf buffer) throws IOException {

        this.cookie = buffer.readLong();
        this.cmd = buffer.readShort();
        this.type = buffer.readShort();

        nioBuffer.clear();
        buffer.readBytes(nioBuffer);
        nioBuffer.flip();

        switch (type) {
            case NameNodeProtocol.REQ_CREATE_FILE:
                this.createFileReq = new RpcRequestMessage.CreateFileReq();
                createFileReq.update(nioBuffer);
                break;
            case NameNodeProtocol.REQ_GET_FILE:
                this.fileReq = new RpcRequestMessage.GetFileReq();
                fileReq.update(nioBuffer);
                break;
            case NameNodeProtocol.REQ_SET_FILE:
                this.setFileReq = new RpcRequestMessage.SetFileReq();
                setFileReq.update(nioBuffer);
                break;
            case NameNodeProtocol.REQ_REMOVE_FILE:
                this.removeReq = new RpcRequestMessage.RemoveFileReq();
                removeReq.update(nioBuffer);
                break;
            case NameNodeProtocol.REQ_RENAME_FILE:
                this.renameFileReq = new RpcRequestMessage.RenameFileReq();
                renameFileReq.update(nioBuffer);
                break;
            case NameNodeProtocol.REQ_GET_BLOCK:
                this.getBlockReq = new RpcRequestMessage.GetBlockReq();
                getBlockReq.update(nioBuffer);
                break;
            case NameNodeProtocol.REQ_GET_LOCATION:
                this.getLocationReq = new RpcRequestMessage.GetLocationReq();
                getLocationReq.update(nioBuffer);
                break;
            case NameNodeProtocol.REQ_SET_BLOCK:
                this.setBlockReq = new RpcRequestMessage.SetBlockReq();
                setBlockReq.update(nioBuffer);
                break;
            case NameNodeProtocol.REQ_GET_DATANODE:
                this.getDataNodeReq = new RpcRequestMessage.GetDataNodeReq();
                getDataNodeReq.update(nioBuffer);
                break;
            case NameNodeProtocol.REQ_DUMP_NAMENODE:
                this.dumpNameNodeReq = new RpcRequestMessage.DumpNameNodeReq();
                dumpNameNodeReq.update(nioBuffer);
                break;
            case NameNodeProtocol.REQ_PING_NAMENODE:
                this.pingNameNodeReq = new RpcRequestMessage.PingNameNodeReq();
                pingNameNodeReq.update(nioBuffer);
                break;
        }
    }

    public short getCmd() {
        return cmd;
    }

    public short getType() {
        return type;
    }

    public RpcRequestMessage.CreateFileReq createFile() {
        return this.createFileReq;
    }

    public RpcRequestMessage.GetFileReq getFile() {
        return fileReq;
    }

    public RpcRequestMessage.SetFileReq setFile() {
        return setFileReq;
    }

    public RpcRequestMessage.RemoveFileReq removeFile() {
        return removeReq;
    }

    public RpcRequestMessage.RenameFileReq renameFile() {
        return renameFileReq;
    }

    public RpcRequestMessage.GetBlockReq getBlock() {
        return getBlockReq;
    }

    public RpcRequestMessage.GetLocationReq getLocation() {
        return getLocationReq;
    }

    public RpcRequestMessage.SetBlockReq setBlock() {
        return setBlockReq;
    }

    public RpcRequestMessage.GetDataNodeReq getDataNode() {
        return this.getDataNodeReq;
    }

    public RpcRequestMessage.DumpNameNodeReq dumpNameNode() {
        return this.dumpNameNodeReq;
    }

    public RpcRequestMessage.PingNameNodeReq pingNameNode() {
        return this.pingNameNodeReq;
    }
}
