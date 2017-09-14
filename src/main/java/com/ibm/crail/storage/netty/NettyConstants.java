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

import com.ibm.crail.conf.CrailConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;

public class NettyConstants {
    public static final String STORAGENODE_NETTY_STORAGE_LIMIT_KEY = "crail.storage.netty.storagelimit";
    public static long STORAGENODE_NETTY_STORAGE_LIMIT = 1073741824;

    public static final String STORAGENODE_NETTY_ALLOCATION_SIZE_KEY = "crail.storage.netty.allocationsize";
    public static long STORAGENODE_NETTY_ALLOCATION_SIZE = 1073741824;

    public static final String STORAGENODE_NETTY_INTERFACE_KEY = "crail.storage.netty.interface";
    public static InetSocketAddress STORAGENODE_NETTY_INTERFACE = null;
    public static String _ifname = "lo";

    public static final String STORAGENODE_NETTY_PORT_KEY = "crail.storage.netty.port";
    public static int STORAGENODE_NETTY_PORT = 19862;

    static public void init(CrailConfiguration conf) throws Exception {

        if (conf.get(STORAGENODE_NETTY_STORAGE_LIMIT_KEY) != null) {
            STORAGENODE_NETTY_STORAGE_LIMIT = Long.parseLong(conf.get(STORAGENODE_NETTY_STORAGE_LIMIT_KEY));
        }

        if (conf.get(STORAGENODE_NETTY_ALLOCATION_SIZE_KEY) != null) {
            STORAGENODE_NETTY_ALLOCATION_SIZE = Long.parseLong(conf.get(STORAGENODE_NETTY_ALLOCATION_SIZE_KEY));
        }

        if(STORAGENODE_NETTY_ALLOCATION_SIZE > STORAGENODE_NETTY_STORAGE_LIMIT) {
            throw new Exception(" Allocation size: " + STORAGENODE_NETTY_ALLOCATION_SIZE +
                    " is greater than the storage limit" + STORAGENODE_NETTY_STORAGE_LIMIT);
        }

        if(STORAGENODE_NETTY_STORAGE_LIMIT % STORAGENODE_NETTY_ALLOCATION_SIZE != 0 ) {
            throw new Exception(" Storage size: " + STORAGENODE_NETTY_STORAGE_LIMIT +
                    " is not a multiple of allocation size " + STORAGENODE_NETTY_ALLOCATION_SIZE);
        }

        /* now setup the interface */
        if (conf.get(STORAGENODE_NETTY_INTERFACE_KEY) != null) {
            _ifname = conf.get(STORAGENODE_NETTY_INTERFACE_KEY);
        }
        if(conf.get(STORAGENODE_NETTY_PORT_KEY) != null) {
            STORAGENODE_NETTY_PORT = Integer.parseInt(conf.get(STORAGENODE_NETTY_PORT_KEY));
        }
    }
}
