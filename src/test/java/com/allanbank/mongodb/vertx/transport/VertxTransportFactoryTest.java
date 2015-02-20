/*
 * #%L
 * VertxTransportFactoryTest.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2015 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.allanbank.mongodb.vertx.transport;

import static com.allanbank.mongodb.vertx.transport.HandlerSupport.handler;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.SocketFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocketFactory;

import org.junit.Test;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.transport.SslEngineFactory;
import com.allanbank.mongodb.client.transport.TransportResponseListener;

/**
 * VertxTransportFactoryTest provides tests for the
 * {@link VertxTransportFactory}.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class VertxTransportFactoryTest {

    /**
     * Test method for {@link VertxTransportFactory#createTransport}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCreateSslTransport() {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setSocketFactory(SSLSocketFactory.getDefault());

        final Vertx mockVertx = createMock(Vertx.class);
        final NetClient mockClient = createMock(NetClient.class);
        final NetSocket mockSocket = createMock(NetSocket.class);
        final TransportResponseListener mockListener = createMock(TransportResponseListener.class);

        expect(mockVertx.createNetClient()).andReturn(mockClient);
        expect(mockClient.setConnectTimeout(config.getConnectTimeout()))
                .andReturn(mockClient);
        expect(mockClient.setTCPKeepAlive(config.isUsingSoKeepalive()))
                .andReturn(mockClient);
        expect(mockClient.setUsePooledBuffers(true)).andReturn(mockClient);
        expect(mockClient.setSSL(true)).andReturn(mockClient);

        expect(mockClient.setTCPNoDelay(true)).andReturn(mockClient);
        expect(
                mockClient.connect(eq(27017), eq("localhost"),
                        handler(mockSocket))).andReturn(mockClient);

        expect(mockSocket.closeHandler(anyObject(VoidHandler.class)))
                .andReturn(mockSocket);
        expect(mockSocket.dataHandler(anyObject(Handler.class))).andReturn(
                mockSocket);
        expect(mockSocket.exceptionHandler(anyObject(Handler.class)))
                .andReturn(mockSocket);

        replay(mockVertx, mockClient, mockSocket, mockListener);

        final VertxTransportFactory factory = new VertxTransportFactory(
                mockVertx);

        final StringEncoderCache eCache = new StringEncoderCache();
        final StringDecoderCache dCache = new StringDecoderCache();
        final Cluster cluster = new Cluster(config, ClusterType.STAND_ALONE);
        final Server server = cluster.add("localhost:27017");

        try {
            final VertxTransport t = factory.createTransport(server, config,
                    eCache, dCache, mockListener);
            assertThat(t, notNullValue());
        }
        catch (final IOException e) {
            fail(e.getMessage());
        }

        verify(mockVertx, mockClient, mockSocket, mockListener);
    }

    /**
     * Test method for {@link VertxTransportFactory#createTransport}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCreateSslTransportForEngine() {
        final SocketFactory sFactory = new MockSslEngineSocketFactory();

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.setSocketFactory(sFactory);

        final Vertx mockVertx = createMock(Vertx.class);
        final NetClient mockClient = createMock(NetClient.class);
        final NetSocket mockSocket = createMock(NetSocket.class);
        final TransportResponseListener mockListener = createMock(TransportResponseListener.class);

        expect(mockVertx.createNetClient()).andReturn(mockClient);
        expect(mockClient.setConnectTimeout(config.getConnectTimeout()))
                .andReturn(mockClient);
        expect(mockClient.setTCPKeepAlive(config.isUsingSoKeepalive()))
                .andReturn(mockClient);
        expect(mockClient.setUsePooledBuffers(true)).andReturn(mockClient);
        expect(mockClient.setSSL(true)).andReturn(mockClient);

        expect(mockClient.setTCPNoDelay(true)).andReturn(mockClient);
        expect(
                mockClient.connect(eq(27017), eq("localhost"),
                        handler(mockSocket))).andReturn(mockClient);

        expect(mockSocket.closeHandler(anyObject(VoidHandler.class)))
                .andReturn(mockSocket);
        expect(mockSocket.dataHandler(anyObject(Handler.class))).andReturn(
                mockSocket);
        expect(mockSocket.exceptionHandler(anyObject(Handler.class)))
                .andReturn(mockSocket);

        replay(mockVertx, mockClient, mockSocket, mockListener);

        final VertxTransportFactory factory = new VertxTransportFactory(
                mockVertx);

        final StringEncoderCache eCache = new StringEncoderCache();
        final StringDecoderCache dCache = new StringDecoderCache();
        final Cluster cluster = new Cluster(config, ClusterType.STAND_ALONE);
        final Server server = cluster.add("localhost:27017");

        try {
            final VertxTransport t = factory.createTransport(server, config,
                    eCache, dCache, mockListener);
            assertThat(t, notNullValue());
        }
        catch (final IOException e) {
            fail(e.getMessage());
        }

        verify(mockVertx, mockClient, mockSocket, mockListener);
    }

    /**
     * Test method for {@link VertxTransportFactory#createTransport}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTransport() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        final Vertx mockVertx = createMock(Vertx.class);
        final NetClient mockClient = createMock(NetClient.class);
        final NetSocket mockSocket = createMock(NetSocket.class);
        final TransportResponseListener mockListener = createMock(TransportResponseListener.class);

        expect(mockVertx.createNetClient()).andReturn(mockClient);
        expect(mockClient.setConnectTimeout(config.getConnectTimeout()))
                .andReturn(mockClient);
        expect(mockClient.setTCPKeepAlive(config.isUsingSoKeepalive()))
                .andReturn(mockClient);
        expect(mockClient.setUsePooledBuffers(true)).andReturn(mockClient);

        expect(mockClient.setTCPNoDelay(true)).andReturn(mockClient);
        expect(
                mockClient.connect(eq(27017), eq("localhost"),
                        handler(mockSocket))).andReturn(mockClient);

        expect(mockSocket.closeHandler(anyObject(VoidHandler.class)))
                .andReturn(mockSocket);
        expect(mockSocket.dataHandler(anyObject(Handler.class))).andReturn(
                mockSocket);
        expect(mockSocket.exceptionHandler(anyObject(Handler.class)))
                .andReturn(mockSocket);

        replay(mockVertx, mockClient, mockSocket, mockListener);

        final VertxTransportFactory factory = new VertxTransportFactory(
                mockVertx);

        final StringEncoderCache eCache = new StringEncoderCache();
        final StringDecoderCache dCache = new StringDecoderCache();
        final Cluster cluster = new Cluster(config, ClusterType.STAND_ALONE);
        final Server server = cluster.add("localhost:27017");

        try {
            final VertxTransport t = factory.createTransport(server, config,
                    eCache, dCache, mockListener);
            assertThat(t, notNullValue());
        }
        catch (final IOException e) {
            fail(e.getMessage());
        }

        verify(mockVertx, mockClient, mockSocket, mockListener);
    }

    /**
     * MockSslEngineSocketFactory provides a fake socket factory that also
     * implements the {@link SslEngineFactory} interface.
     *
     * @api.no This class is <b>NOT</b> part of the drivers API. This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver.
     * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected final class MockSslEngineSocketFactory extends SocketFactory
            implements SslEngineFactory {
        /**
         * {@inheritDoc}
         */
        @Override
        public Socket createSocket(final InetAddress host, final int port)
                throws IOException {
            return null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Socket createSocket(final InetAddress address, final int port,
                final InetAddress localAddress, final int localPort)
                        throws IOException {
            return null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Socket createSocket(final String host, final int port)
                throws IOException, UnknownHostException {
            return null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Socket createSocket(final String host, final int port,
                final InetAddress localHost, final int localPort)
                        throws IOException, UnknownHostException {
            return null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SSLEngine createSSLEngine() {
            return null;
        }
    }
}
