package com.ibm.crail.storage.netty;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.storage.StorageClient;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.netty.client.NettyEndpointGroup;
import com.ibm.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by atr on 08.09.17.
 */

public class NettyStorageClient implements StorageClient {
    static private final Logger LOG = CrailNettyUtils.getLogger();
    /* client-side stuff */
    NettyEndpointGroup epGroup;

    NettyStorageClient(){
        this.epGroup = null;
    }

    public StorageEndpoint createEndpoint(DataNodeInfo dataNodeInfo) throws IOException {
        InetSocketAddress addr = CrailUtils.datanodeInfo2SocketAddr(dataNodeInfo);
        LOG.debug(" Opening a connection to StorageNode: " + addr.toString());
        return epGroup.createEndpoint(addr);
    }

    public void close() throws Exception {
        synchronized (this) {
            if (this.epGroup != null) {
                this.epGroup.close();
                this.epGroup = null;
            }
        }
    }

    public void init(CrailConfiguration crailConfiguration, String[] strings) throws Exception {
        //TODO: parse client side parameters
        synchronized(this) {
            if (epGroup == null) {
                epGroup = new NettyEndpointGroup();
            }
        }
    }

    public void printConf(Logger logger) {
        //TODO: show client side parameters
    }
}
