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
import org.apache.commons.cli.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class NettyConstants {
    private String STORAGENODE_NETTY_STORAGE_LIMIT_KEY = "crail.storage.netty.storagelimit";
    private long STORAGENODE_NETTY_STORAGE_LIMIT = 1073741824;

    private String STORAGENODE_NETTY_ALLOCATION_SIZE_KEY = "crail.storage.netty.allocationsize";
    private long STORAGENODE_NETTY_ALLOCATION_SIZE = 1073741824;

    private HashMap<String, String> faultyMap = new LinkedHashMap<>();

    private String STORAGENODE_NETTY_ADDRESS_KEY = "crail.storage.netty.address";
    private InetSocketAddress STORAGENODE_NETTY_ADDRESS = null;
    private String _ipaddress = null;

    private String STORAGENODE_NETTY_PORT_KEY = "crail.storage.netty.port";
    private int STORAGENODE_NETTY_PORT = 19862;
    private Options options;

    private static NettyConstants _conf = null;

    public static synchronized NettyConstants get() {
        if(_conf == null) {
            _conf = new NettyConstants();
        }
        return _conf;
    }

    private NettyConstants() {
        this.options = new Options();
        options.addOption("h", "help", false, "show this help");
        options.addOption("a", "address", true, "(string) IP address or the hostname, where to run the server ");
        options.addOption("p", "port", true, "(int) port number of the netty data server");
        options.addOption("s", "allocationSize", true, "(long) allocation size ");
        options.addOption("l", "storageLimit", true, "(long) storage limit");
        try {
            _ipaddress = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private void show_help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options);
    }

    private void errorAbort(String str) {
        show_help();
        System.err.println("************ ERROR (see help on stdout ) *******************");
        System.err.println(str);
        System.err.println("**************************************");
        System.exit(-1);
    }

    public String printConf() {
        StringBuilder sb = new StringBuilder();
        sb.append(new String(" [NETTY]  address   : " + STORAGENODE_NETTY_ADDRESS + " \n"));
        sb.append(new String(" [NETTY]  limit     : " + STORAGENODE_NETTY_STORAGE_LIMIT + " \n"));
        sb.append(new String(" [NETTY]  allocSize : " + STORAGENODE_NETTY_ALLOCATION_SIZE + " \n"));
        return sb.toString();
    }

    public long getStorageLimit() {
        return this.STORAGENODE_NETTY_STORAGE_LIMIT;
    }

    public long getAllocationSize() {
        return this.STORAGENODE_NETTY_ALLOCATION_SIZE;
    }

    public InetSocketAddress getNettyDataNodeAddress() throws Exception {
        if(null == STORAGENODE_NETTY_ADDRESS) {
            InetAddress addr = InetAddress.getByName(_ipaddress);
            STORAGENODE_NETTY_ADDRESS = new InetSocketAddress(addr, STORAGENODE_NETTY_PORT);
        }
        /* once you have it always return it */
        return STORAGENODE_NETTY_ADDRESS;
    }

    private void checkDeprecatedProperties(final CrailConfiguration conf) throws Exception {
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

    public void init(CrailConfiguration conf, String[] args) throws Exception {
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

        /* the args are given priority */
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                show_help();
                System.exit(0);
            }
            if(cmd.hasOption("a")){
                _ipaddress = cmd.getOptionValue("a").trim();
                // we just mark it null to that next time it is re-evaluated
                STORAGENODE_NETTY_ADDRESS = null;
            }
            if(cmd.hasOption("p")){
                STORAGENODE_NETTY_PORT = Integer.parseInt(cmd.getOptionValue("p").trim());
                // we just mark it null to that next time it is re-evaluated
                STORAGENODE_NETTY_ADDRESS = null;
            }
            if(cmd.hasOption("s")){
                STORAGENODE_NETTY_ALLOCATION_SIZE = Long.parseLong(cmd.getOptionValue("s").trim());
            }
            if(cmd.hasOption("l")){
                STORAGENODE_NETTY_STORAGE_LIMIT = Long.parseLong(cmd.getOptionValue("l").trim());
            }
        } catch (ParseException e) {
            errorAbort("Failed to parse command line properties" + e);
        }
    }
}
