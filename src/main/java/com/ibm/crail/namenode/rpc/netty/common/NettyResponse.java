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
import com.ibm.crail.namenode.rpc.RpcNameNodeState;
import com.ibm.crail.namenode.rpc.RpcResponseMessage;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public class NettyResponse implements RpcNameNodeState {
    public static final int CSIZE = 12 + Math.max(RpcResponseMessage.GetBlockRes.CSIZE, RpcResponseMessage.RenameRes.CSIZE);

    private short type;
    private short error;
    private long cookie;
    private RpcResponseMessage.VoidRes voidRes;
    private RpcResponseMessage.CreateFileRes createFileRes;
    private RpcResponseMessage.GetFileRes getFileRes;
    private RpcResponseMessage.DeleteFileRes delFileRes;
    private RpcResponseMessage.RenameRes renameRes;
    private RpcResponseMessage.GetBlockRes getBlockRes;
    private RpcResponseMessage.GetLocationRes getLocationRes;
    private RpcResponseMessage.GetDataNodeRes getDataNodeRes;
    private RpcResponseMessage.PingNameNodeRes pingNameNodeRes;
    private ByteBuffer nioBuffer;
    private NettyCommonFuture resp;

    public NettyCommonFuture getNettyCommonFuture(){
        return resp;
    }

    public NettyResponse() {
        this.type = 0;
        this.error = 0;
        this.cookie = 0;
        this.nioBuffer = ByteBuffer.allocateDirect(NettyResponse.CSIZE - 12);
    }

    private NettyResponse(short type, long cookie, NettyCommonFuture resp) {
        this();
        this.type  = type;
        this.cookie = cookie;
        this.resp = resp;
    }

    public NettyResponse(RpcResponseMessage.VoidRes message, long cookie, NettyCommonFuture resp) {
        this(message.getType(), cookie, resp);
        this.voidRes = message;
    }

    public NettyResponse(RpcResponseMessage.CreateFileRes message, long cookie, NettyCommonFuture resp) {
        this(message.getType(), cookie, resp);
        this.createFileRes = message;
    }

    public NettyResponse(RpcResponseMessage.GetFileRes message, long cookie, NettyCommonFuture resp) {
        this(message.getType(), cookie, resp);
        this.getFileRes = message;
    }

    public NettyResponse(RpcResponseMessage.DeleteFileRes message, long cookie, NettyCommonFuture resp) {
        this(message.getType(), cookie, resp);
        this.delFileRes = message;
    }

    public NettyResponse(RpcResponseMessage.RenameRes message, long cookie, NettyCommonFuture resp) {
        this(message.getType(), cookie, resp);
        this.renameRes = message;
    }

    public NettyResponse(RpcResponseMessage.GetBlockRes message, long cookie, NettyCommonFuture resp) {
        this(message.getType(), cookie, resp);
        this.getBlockRes = message;
    }

    public NettyResponse(RpcResponseMessage.GetLocationRes message, long cookie, NettyCommonFuture resp) {
        this(message.getType(), cookie, resp);
        this.getLocationRes = message;
    }

    public NettyResponse(RpcResponseMessage.GetDataNodeRes message, long cookie, NettyCommonFuture resp) {
        this(message.getType(), cookie, resp);
        this.getDataNodeRes = message;
    }

    public NettyResponse(RpcResponseMessage.PingNameNodeRes message, long cookie, NettyCommonFuture resp) {
        this(message.getType(), cookie, resp);
        this.pingNameNodeRes = message;
    }

    public void setTypeAndAllocate(short type, long cookie) throws Exception {
        this.type = type;
        this.cookie = cookie;
        switch(type){
            case NameNodeProtocol.RES_VOID:
                this.voidRes = new RpcResponseMessage.VoidRes();
                break;
            case NameNodeProtocol.RES_CREATE_FILE:
                this.createFileRes = new RpcResponseMessage.CreateFileRes();
                break;
            case NameNodeProtocol.RES_GET_FILE:
                this.getFileRes = new RpcResponseMessage.GetFileRes();
                break;
            case NameNodeProtocol.RES_DELETE_FILE:
                this.delFileRes = new RpcResponseMessage.DeleteFileRes();
                break;
            case NameNodeProtocol.RES_RENAME_FILE:
                this.renameRes = new RpcResponseMessage.RenameRes();
                break;
            case NameNodeProtocol.RES_GET_BLOCK:
                this.getBlockRes = new RpcResponseMessage.GetBlockRes();
                break;
            case NameNodeProtocol.RES_GET_LOCATION:
                this.getLocationRes = new RpcResponseMessage.GetLocationRes();
                break;
            case NameNodeProtocol.RES_GET_DATANODE:
                this.getDataNodeRes = new RpcResponseMessage.GetDataNodeRes();
                break;
            case NameNodeProtocol.RES_PING_NAMENODE:
                this.pingNameNodeRes = new RpcResponseMessage.PingNameNodeRes();
                break;
        }
    }

    public int size(){
        return CSIZE;
    }

    public long getCookie(){
        return cookie;
    }
    public int write(ByteBuf buffer){
        buffer.writeLong(cookie);
        buffer.writeShort(type);
        buffer.writeShort(error);

        int written = 12;
        nioBuffer.clear();
        switch(type){
            case NameNodeProtocol.RES_VOID:
                written += voidRes.write(nioBuffer);
                break;
            case NameNodeProtocol.RES_CREATE_FILE:
                written += createFileRes.write(nioBuffer);
                break;
            case NameNodeProtocol.RES_GET_FILE:
                written += getFileRes.write(nioBuffer);
                break;
            case NameNodeProtocol.RES_DELETE_FILE:
                written += delFileRes.write(nioBuffer);
                break;
            case NameNodeProtocol.RES_RENAME_FILE:
                written += renameRes.write(nioBuffer);
                break;
            case NameNodeProtocol.RES_GET_BLOCK:
                written += getBlockRes.write(nioBuffer);
                break;
            case NameNodeProtocol.RES_GET_LOCATION:
                written += getLocationRes.write(nioBuffer);
                break;
            case NameNodeProtocol.RES_GET_DATANODE:
                written += getDataNodeRes.write(nioBuffer);
                break;
            case NameNodeProtocol.RES_PING_NAMENODE:
                written += pingNameNodeRes.write(nioBuffer);
                break;
        }
        /* reset and copy becuase we want to copy the whole capacity of the nio buffer */
        nioBuffer.clear();
        buffer.writeBytes(nioBuffer);
        return written;
    }

    public void update(long cookie, ByteBuf buffer){
        assert this.cookie == cookie;
        this.type = buffer.readShort();
        this.error = buffer.readShort();

        nioBuffer.clear();
        buffer.readBytes(nioBuffer);
        nioBuffer.flip();
        switch(type){
            case NameNodeProtocol.RES_VOID:
                //this.voidRes = new RpcResponseMessage.VoidRes();
                voidRes.update(nioBuffer);
                voidRes.setError(error);
                break;
            case NameNodeProtocol.RES_CREATE_FILE:
                //this.createFileRes = new RpcResponseMessage.CreateFileRes();
                createFileRes.update(nioBuffer);
                createFileRes.setError(error);
                break;
            case NameNodeProtocol.RES_GET_FILE:
                //this.getFileRes = new RpcResponseMessage.GetFileRes();
                getFileRes.update(nioBuffer);
                getFileRes.setError(error);
                break;
            case NameNodeProtocol.RES_DELETE_FILE:
                //this.delFileRes = new RpcResponseMessage.DeleteFileRes();
                delFileRes.update(nioBuffer);
                delFileRes.setError(error);
                break;
            case NameNodeProtocol.RES_RENAME_FILE:
                //this.renameRes = new RpcResponseMessage.RenameRes();
                renameRes.update(nioBuffer);
                renameRes.setError(error);
                break;
            case NameNodeProtocol.RES_GET_BLOCK:
                //this.getBlockRes = new RpcResponseMessage.GetBlockRes();
                getBlockRes.update(nioBuffer);
                getBlockRes.setError(error);
                break;
            case NameNodeProtocol.RES_GET_LOCATION:
                //this.getLocationRes = new RpcResponseMessage.GetLocationRes();
                getLocationRes.update(nioBuffer);
                getLocationRes.setError(error);
                break;
            case NameNodeProtocol.RES_GET_DATANODE:
                //this.getDataNodeRes = new RpcResponseMessage.GetDataNodeRes();
                getDataNodeRes.update(nioBuffer);
                getDataNodeRes.setError(error);
                break;
            case NameNodeProtocol.RES_PING_NAMENODE:
                //this.pingNameNodeRes = new RpcResponseMessage.PingNameNodeRes();
                pingNameNodeRes.update(nioBuffer);
                pingNameNodeRes.setError(error);
                break;
        }
    }

    public short getType(){
        return type;
    }

    public short getError() {
        return error;
    }

    public void setError(short error) {
        this.error = error;
    }

    public RpcResponseMessage.VoidRes getVoid() {
        return voidRes;
    }

    public RpcResponseMessage.CreateFileRes createFile() {
        return createFileRes;
    }

    public RpcResponseMessage.GetFileRes getFile() {
        return getFileRes;
    }

    public RpcResponseMessage.DeleteFileRes delFile() {
        return delFileRes;
    }

    public RpcResponseMessage.RenameRes getRename() {
        return renameRes;
    }

    public RpcResponseMessage.GetBlockRes getBlock() {
        return getBlockRes;
    }

    public RpcResponseMessage.GetLocationRes getLocation() {
        return getLocationRes;
    }

    public RpcResponseMessage.GetDataNodeRes getDataNode() {
        return getDataNodeRes;
    }

    public RpcResponseMessage.PingNameNodeRes pingNameNode(){
        return this.pingNameNodeRes;
    }
}

