package com.ibm.crail.namenode.rpc.netty.client;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.rpc.RpcClient;
import com.ibm.crail.rpc.RpcConnection;
import org.slf4j.Logger;

import java.net.InetSocketAddress;

/**
 * Created by atr on 08.09.17.
 */
public class NettyRpcNamenodeClient implements RpcClient {
    private NettyRPCNamenodeClientGroup clientGroup;

    public NettyRpcNamenodeClient(){
        this.clientGroup = null;
    }

    public RpcConnection connect(InetSocketAddress inetSocketAddress) throws Exception {
        return clientGroup.getClient(inetSocketAddress);
    }

    public void close() {
        if(this.clientGroup!=null){
            this.clientGroup.closeClientGroup();
            this.clientGroup = null;
        }
    }

    public void init(CrailConfiguration crailConfiguration, String[] strings) throws Exception {
        /* here we init it */
        this.clientGroup = new NettyRPCNamenodeClientGroup();
    }

    public void printConf(Logger logger) {

    }
}
