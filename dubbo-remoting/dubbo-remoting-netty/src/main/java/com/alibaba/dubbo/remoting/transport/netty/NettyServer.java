/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.remoting.transport.netty;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ExecutorUtil;
import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.Server;
import com.alibaba.dubbo.remoting.transport.AbstractServer;
import com.alibaba.dubbo.remoting.transport.dispatcher.ChannelHandlers;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * NettyServer
 */
public class NettyServer extends AbstractServer implements Server {

    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);

    private Map<String, Channel> channels; // <ip:port, channel>

    private ServerBootstrap bootstrap;

    private org.jboss.netty.channel.Channel channel;

    public NettyServer(URL url, ChannelHandler handler) throws RemotingException {
        super(url, ChannelHandlers.wrap(handler, ExecutorUtil.setThreadName(url, SERVER_THREAD_POOL_NAME)));
    }

    /**
     * NettyServer 建立网络连接
     *
     * 熟悉本方法需要具备Netty的知识，有关源码：阅读Netty系列文章，这里不对每一行代码进行解读，本方法@1、@2引起了我的注意，
     * 首先创建NettyServer必须传入一个服务提供者URL，但从DubboProtocol#createServer中可以看出，Server是基于网络套接字（ip:port）缓存的，
     * 一个JVM应用中，必然会存在多个dubbo:server标签，就会有多个URL，这里为什么可以这样做呢？
     * 从DubboProtocol#createServer中可以看出，在解析第二个dubbo:service标签时并不会调用createServer,而是会调用Server#reset方法，
     * 是不是这个方法有什么魔法，在reset方法时能将URL也注册到Server上，那接下来分析NettyServer#reset方法是如何实现的。
     */
    @Override
    protected void doOpen() throws Throwable {
        NettyHelper.setNettyLoggerFactory();
        ExecutorService boss = Executors.newCachedThreadPool(new NamedThreadFactory("NettyServerBoss", true));
        ExecutorService worker = Executors.newCachedThreadPool(new NamedThreadFactory("NettyServerWorker", true));
        ChannelFactory channelFactory = new NioServerSocketChannelFactory(boss, worker, getUrl().getPositiveParameter(Constants.IO_THREADS_KEY, Constants.DEFAULT_IO_THREADS));
        bootstrap = new ServerBootstrap(channelFactory);

        final NettyHandler nettyHandler = new NettyHandler(getUrl(), this); // @1
        channels = nettyHandler.getChannels();
        // https://issues.jboss.org/browse/NETTY-365
        // https://issues.jboss.org/browse/NETTY-379
        // final Timer timer = new HashedWheelTimer(new NamedThreadFactory("NettyIdleTimer", true));
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() {
                NettyCodecAdapter adapter = new NettyCodecAdapter(getCodec(), getUrl(), NettyServer.this);
                ChannelPipeline pipeline = Channels.pipeline();
                /*int idleTimeout = getIdleTimeout();
                if (idleTimeout > 10000) {
                    pipeline.addLast("timer", new IdleStateHandler(timer, idleTimeout / 1000, 0, 0));
                }*/
                pipeline.addLast("decoder", adapter.getDecoder());
                pipeline.addLast("encoder", adapter.getEncoder());
                pipeline.addLast("handler", nettyHandler); // @2
                return pipeline;
            }
        });
        // bind
        channel = bootstrap.bind(getBindAddress());
    }

    @Override
    protected void doClose() throws Throwable {
        try {
            if (channel != null) {
                // unbind.
                channel.close();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            Collection<com.alibaba.dubbo.remoting.Channel> channels = getChannels();
            if (channels != null && !channels.isEmpty()) {
                for (com.alibaba.dubbo.remoting.Channel channel : channels) {
                    try {
                        channel.close();
                    } catch (Throwable e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            if (bootstrap != null) {
                // release external resource.
                bootstrap.releaseExternalResources();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            if (channels != null) {
                channels.clear();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
    }

    @Override
    public Collection<Channel> getChannels() {
        Collection<Channel> chs = new HashSet<Channel>();
        for (Channel channel : this.channels.values()) {
            if (channel.isConnected()) {
                chs.add(channel);
            } else {
                channels.remove(NetUtils.toAddressString(channel.getRemoteAddress()));
            }
        }
        return chs;
    }

    @Override
    public Channel getChannel(InetSocketAddress remoteAddress) {
        return channels.get(NetUtils.toAddressString(remoteAddress));
    }

    @Override
    public boolean isBound() {
        return channel.isBound();
    }

}
