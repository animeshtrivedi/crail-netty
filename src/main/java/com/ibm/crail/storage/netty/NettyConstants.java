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

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class NettyConstants {
    public static final String STORAGENODE_NETTY_STORAGE_LIMIT_KEY = "crail.storage.netty.storagelimit";
    public static long STORAGENODE_NETTY_STORAGE_LIMIT = 1073741824;

    public static final String STORAGENODE_NETTY_ALLOCATION_SIZE_KEY = "crail.storage.netty.allocationsize";
    public static long STORAGENODE_NETTY_ALLOCATION_SIZE = 1073741824;

    static final HashMap<String, String> faultyMap = new LinkedHashMap<String, String>();

    public static final String STORAGENODE_NETTY_ADDRESS_KEY = "crail.storage.netty.address";
    public static InetSocketAddress STORAGENODE_NETTY_ADDRESS = null;
    public static String _ipaddress = "127.0.0.1";

    public static final String STORAGENODE_NETTY_PORT_KEY = "crail.storage.netty.port";
    public static int STORAGENODE_NETTY_PORT = 19862;

    static private void checkDeprecatedProperties(final CrailConfiguration conf) throws Exception {
        StringBuilder sb = new StringBuilder();
        int sum = 0;
        for (Object o : faultyMap.entrySet()) {
            Map.Entry pair = (Map.Entry) o;
            String key = (String) pair.getKey();
            String value = (String) pair.getValue();
            if(conf.get(key) != null){
                /* an old property is set */
                sb.append("\n \"" + key + "\" is deprecated and removed. Please use \"" + value + "\" to set the property\n");
                sum++;
            }
        }
        if(sum != 0){
            /* we had some old values set */
            throw new Exception(sb.toString());
        }
    }

    static public void init(CrailConfiguration conf) throws Exception {
        /* check for old values and error the user */
        faultyMap.put("crail.storage.netty.interface", STORAGENODE_NETTY_ADDRESS_KEY);
        faultyMap.put("crail.datanode.netty.storagelimit", STORAGENODE_NETTY_STORAGE_LIMIT_KEY);
        faultyMap.put("crail.datanode.netty.allocationsize", STORAGENODE_NETTY_ALLOCATION_SIZE_KEY);
        faultyMap.put("crail.datanode.netty.interface", STORAGENODE_NETTY_ADDRESS_KEY);
        faultyMap.put("crail.datanode.netty.port", STORAGENODE_NETTY_PORT_KEY);
        checkDeprecatedProperties(conf);

        /* now we set the new values */
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
        if (conf.get(STORAGENODE_NETTY_ADDRESS_KEY) != null) {
            _ipaddress = conf.get(STORAGENODE_NETTY_ADDRESS_KEY);
        }
        if(conf.get(STORAGENODE_NETTY_PORT_KEY) != null) {
            STORAGENODE_NETTY_PORT = Integer.parseInt(conf.get(STORAGENODE_NETTY_PORT_KEY));
        }
    }
}
