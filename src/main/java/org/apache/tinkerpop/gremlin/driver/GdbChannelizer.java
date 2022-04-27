/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.handler.*;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Client-side channel initializer interface.  It is responsible for constructing the Netty {@code ChannelPipeline}
 * used by the client to connect and send message to Gremlin Server.
 */
public interface GdbChannelizer extends ChannelHandler {

    /**
     * Initializes the {@code GdbChannelizer}. Called just after construction.
     */
    public void init(final GdbConnection connection);

    /**
     * Called on {@link GdbConnection#close()} to perform an {@code GdbChannelizer} specific functions.  Note that the
     * {@link GdbConnection} already calls {@code Channel.close()} so there is no need to call that method here.
     * An implementation will typically use this method to send a {@code GdbChannelizer} specific message to the
     * server to notify of shutdown coming from the client side (e.g. a "close" websocket frame).
     */
    public void close(final Channel channel);

    /**
     * Create a message for the driver to use as a "keep-alive" for the connection. This method will only be used if
     * {@link #supportsKeepAlive()} is {@code true}.
     */
    public default Object createKeepAliveMessage() {
        return null;
    }

    /**
     * Determines if the channelizer supports a method for keeping the connection to the server alive.
     */
    public default boolean supportsKeepAlive() {
        return false;
    }

    /**
     * Called after the channel connects. The {@code GdbChannelizer} may need to perform some functions, such as a
     * handshake.
     */
    public default void connected() {
    }

    /**
     * Base implementation of the client side {@link GdbChannelizer}.
     */
    abstract class AbstractChannelizer extends ChannelInitializer<SocketChannel> implements GdbChannelizer {
        protected GdbConnection connection;
        protected GdbCluster cluster;
        private ConcurrentMap<UUID, ResultQueue> pending;

        protected static final String PIPELINE_GREMLIN_SASL_HANDLER = "gremlin-sasl-handler";
        protected static final String PIPELINE_GREMLIN_HANDLER = "gremlin-handler";

        public boolean supportsSsl() {
            return cluster.connectionPoolSettings().enableSsl;
        }

        public abstract void configure(final ChannelPipeline pipeline);

        public void finalize(final ChannelPipeline pipeline) {
            // do nothing
        }

        @Override
        public void close(final Channel channel) {
            // do nothing
        }

        @Override
        public void init(final GdbConnection connection) {
            this.connection = connection;
            this.cluster = connection.getCluster();
            this.pending = connection.getPending();
        }

        @Override
        protected void initChannel(final SocketChannel socketChannel) throws Exception {
            final ChannelPipeline pipeline = socketChannel.pipeline();
            final Optional<SslContext> sslCtx;
            if (supportsSsl()) {
                try {
                    sslCtx = Optional.of(cluster.createSSLContext());
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                sslCtx = Optional.empty();
            }

            if (sslCtx.isPresent()) {
                pipeline.addLast(sslCtx.get().newHandler(socketChannel.alloc(), connection.getUri().getHost(), connection.getUri().getPort()));
            }

            configure(pipeline);
            pipeline.addLast(PIPELINE_GREMLIN_SASL_HANDLER, new GdbHandler.GremlinSaslAuthenticationHandler(cluster.authProperties()));
            pipeline.addLast(PIPELINE_GREMLIN_HANDLER, new GdbHandler.GremlinResponseHandler(pending));
        }
    }

    /**
     * WebSocket {@link GdbChannelizer} implementation.
     */
    public final class WebSocketChannelizer extends AbstractChannelizer {
        private WebSocketClientHandler handler;

        private WebSocketGremlinRequestEncoder webSocketGremlinRequestEncoder;
        private WebSocketGremlinResponseDecoder webSocketGremlinResponseDecoder;

        @Override
        public void init(final GdbConnection connection) {
            super.init(connection);
            webSocketGremlinRequestEncoder = new WebSocketGremlinRequestEncoder(true, cluster.getSerializer());
            webSocketGremlinResponseDecoder = new WebSocketGremlinResponseDecoder(cluster.getSerializer());
        }

        /**
         * Keep-alive is supported through the ping/pong websocket protocol.
         *
         * @see <a href=https://tools.ietf.org/html/rfc6455#section-5.5.2>IETF RFC 6455</a>
         */
        @Override
        public boolean supportsKeepAlive() {
            return true;
        }

        @Override
        public Object createKeepAliveMessage() {
            return new PingWebSocketFrame();
        }

        /**
         * Sends a {@code CloseWebSocketFrame} to the server for the specified channel.
         */
        @Override
        public void close(final Channel channel) {
            if (channel.isOpen()) {
                channel.writeAndFlush(new CloseWebSocketFrame());
            }
        }

        @Override
        public boolean supportsSsl() {
            final String scheme = connection.getUri().getScheme();
            return "wss".equalsIgnoreCase(scheme);
        }

        @Override
        public void configure(final ChannelPipeline pipeline) {
            final String scheme = connection.getUri().getScheme();
            if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme)) {
                throw new IllegalStateException("Unsupported scheme (only ws: or wss: supported): " + scheme);
            }

            if (!supportsSsl() && "wss".equalsIgnoreCase(scheme)) {
                throw new IllegalStateException("To use wss scheme ensure that enableSsl is set to true in configuration");
            }

            final int maxContentLength = cluster.connectionPoolSettings().maxContentLength;
            handler = new WebSocketClientHandler(
                    WebSocketClientHandshakerFactory.newHandshaker(
                            connection.getUri(), WebSocketVersion.V13, null, false, EmptyHttpHeaders.INSTANCE, maxContentLength));

            pipeline.addLast("http-codec", new HttpClientCodec());
            pipeline.addLast("aggregator", new HttpObjectAggregator(maxContentLength));
            pipeline.addLast("ws-handler", handler);
            pipeline.addLast("gremlin-encoder", webSocketGremlinRequestEncoder);
            pipeline.addLast("gremlin-decoder", webSocketGremlinResponseDecoder);
        }

        @Override
        public void connected() {
            try {
                // block for a few seconds - if the handshake takes longer than there's gotta be issues with that
                // server. more than likely, SSL is enabled on the server, but the client forgot to enable it or
                // perhaps the server is not configured for websockets.
                handler.handshakeFuture().get(15000, TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                throw new RuntimeException(new ConnectionException(connection.getUri(),
                        "Could not complete websocket handshake - ensure that client protocol matches server", ex));
            }
        }
    }

    /**
     * NIO {@link GdbChannelizer} implementation.
     */
    public final class NioChannelizer extends AbstractChannelizer {
        @Override
        public void init(final GdbConnection connection) {
            super.init(connection);
        }

        @Override
        public void configure(ChannelPipeline pipeline) {
            pipeline.addLast("gremlin-decoder", new NioGremlinResponseDecoder(cluster.getSerializer()));
            pipeline.addLast("gremlin-encoder", new NioGremlinRequestEncoder(true, cluster.getSerializer()));
        }
    }
}
