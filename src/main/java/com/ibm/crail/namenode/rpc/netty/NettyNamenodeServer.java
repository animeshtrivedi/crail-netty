package com.ibm.crail.namenode.rpc.netty;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.namenode.rpc.netty.common.RequestDecoder;
import com.ibm.crail.namenode.rpc.netty.common.ResponseEncoder;
import com.ibm.crail.rpc.RpcNameNodeService;
import com.ibm.crail.rpc.RpcServer;
import com.ibm.crail.storage.netty.CrailNettyUtils;
import com.ibm.crail.utils.CrailUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;

import java.net.InetSocketAddress;

/**
 * Created by atr on 08.09.17.
 */
public class NettyNamenodeServer extends RpcServer {
    static private Logger LOG = CrailNettyUtils.getLogger();
    RpcNameNodeService service;

    public NettyNamenodeServer(RpcNameNodeService service){
        this.service = service;
    }

    public void run() {
        /* here we run the incoming RPC service */
        InetSocketAddress inetSocketAddress = CrailUtils.getNameNodeAddress();
        LOG.info("Starting the NettyNamenode service at : " + inetSocketAddress);
        /* start the netty server */
        EventLoopGroup acceptGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap boot = new ServerBootstrap();
            boot.group(acceptGroup, workerGroup);
            /* we use sockets */
            boot.channel(NioServerSocketChannel.class);
            /* for new incoming connection */
            boot.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    LOG.info("A new connection has arrived from : " + ch.remoteAddress().toString());
                            /* incoming pipeline */
                    ch.pipeline().addLast("RequestDecoder" , new RequestDecoder());
                    ch.pipeline().addLast("NNProcessor", new NamenodeProcessor(service));
                            /* outgoing pipeline */
                    ch.pipeline().addLast("ResponseEncoder", new ResponseEncoder());
                }
            });
            /* general optimization settings */
            boot.option(ChannelOption.SO_BACKLOG, 1024);
            boot.childOption(ChannelOption.SO_KEEPALIVE, true);

            /* now we bind the server and start */
            ChannelFuture f = boot.bind(inetSocketAddress.getAddress(),
                    inetSocketAddress.getPort()).sync();
            /* at this point we are binded and ready */
            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
            acceptGroup.shutdownGracefully();
            LOG.info("Netty namenode at " + inetSocketAddress + " is shut down");
        }
    }

    public void init(CrailConfiguration crailConfiguration, String[] strings) throws Exception {
        /* for now these are no-ops */
    }

    public void printConf(Logger logger) {
        //TODO
    }
}
