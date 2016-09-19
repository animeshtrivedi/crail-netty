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

package com.ibm.crail.datanode.netty.rpc;

public class MessageTypes{
    static public int WRITE_REQ = 1;
    static public int WRITE_RESP = 2;
    static public int READ_REQ = 3;
    static public int READ_RESP = 4;


    public static String MessageTypeToString(int m){
        switch(m){
            case 1: return "WRTIE_REQ";
            case 2: return "WRITE_RESP";
            case 3: return "READ_REQ";
            case 4: return "READ_RESP";
            default: return ("<INVALID : " + m + " >");
        }
    }

    public static boolean isTypeIllegal(int m){
        switch(m){
            case 1:
            case 2:
            case 3:
            case 4: return false;
        }
        return true;
    }
}