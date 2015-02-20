/*
 * #%L
 * VertxTransportTest.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.util.IOUtils;

/**
 * VertxTransportTest provides tests for the {@link VertxTransport} class.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class VertxTransportTest {

    /** The MongoDB configuration. */
    private MongoClientConfiguration myConfig = null;

    /** The mock for the {@link NetClient}. */
    private NetClient myMockClient = null;

    /** The mock for the {@link TransportResponseListener}. */
    private TransportResponseListener myMockListener = null;

    /** The mock for the {@link NetSocket}. */
    private NetSocket myMockSocket = null;

    /** The server to connect to. */
    private Server myServer = null;

    /**
     * Create support objects.
     */
    @Before
    public void setUp() {
        myConfig = new MongoClientConfiguration();
        myServer = new Cluster(myConfig, ClusterType.STAND_ALONE)
                .add("localhost:27017");
    }

    /**
     * Releases the mocks and other objects.
     */
    @After
    public void tearDown() {
        myConfig = null;
        myServer = null;
        myMockClient = null;
        myMockSocket = null;
        myMockListener = null;
    }

    /**
     * Test method for {@link VertxTransport#close()}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testClose() throws IOException {

        createConstructorMocks();

        myMockSocket.close();
        expectLastCall();

        replay();

        final VertxTransport transport = new VertxTransport(myServer, myConfig,
                new StringEncoderCache(), new StringDecoderCache(),
                myMockListener, myMockClient);

        transport.close();

        verify();
    }

    /**
     * Test method for {@link VertxTransport#VertxTransport}.
     */
    @Test
    public void testCreateFailsToConnect() {
        final IOException toThrow = new IOException("failure");

        myMockClient = createMock(NetClient.class);
        myMockSocket = createMock(NetSocket.class);
        myMockListener = createMock(TransportResponseListener.class);

        expect(myMockClient.setTCPNoDelay(true)).andReturn(myMockClient);

        expect(
                myMockClient.connect(eq(27017), eq("localhost"),
                        handlerSocket(toThrow))).andReturn(myMockClient);

        replay();

        VertxTransport transport = null;
        try {
            transport = new VertxTransport(myServer, myConfig,
                    new StringEncoderCache(), new StringDecoderCache(),
                    myMockListener, myMockClient);
        }
        catch (final IOException caught) {
            assertThat(caught, sameInstance(toThrow));
        }
        finally {
            IOUtils.close(transport);
        }

        verify();
    }

    /**
     * Test method for {@link VertxTransport#VertxTransport}.
     */
    @Test
    public void testCreateFailsToConnectWithRuntimeException() {
        final Throwable toThrow = new RuntimeException("failure");

        myMockClient = createMock(NetClient.class);
        myMockSocket = createMock(NetSocket.class);
        myMockListener = createMock(TransportResponseListener.class);

        expect(myMockClient.setTCPNoDelay(true)).andReturn(myMockClient);

        expect(
                myMockClient.connect(eq(27017), eq("localhost"),
                        handlerSocket(toThrow))).andReturn(myMockClient);

        replay();

        VertxTransport transport = null;
        try {
            transport = new VertxTransport(myServer, myConfig,
                    new StringEncoderCache(), new StringDecoderCache(),
                    myMockListener, myMockClient);
        }
        catch (final IOException caught) {
            assertThat(caught.getCause(), sameInstance(toThrow));
        }
        finally {
            IOUtils.close(transport);
        }

        verify();
    }

    /**
     * Test method for {@link VertxTransport#VertxTransport}.
     */
    @Test
    public void testCreateOnNoAddresses() {

        myMockClient = createMock(NetClient.class);
        myMockSocket = createMock(NetSocket.class);
        myMockListener = createMock(TransportResponseListener.class);
        final Server mockServer = createMock(Server.class);

        final List<InetSocketAddress> addresses = Collections.emptyList();
        expect(mockServer.getAddresses()).andReturn(addresses).times(2);

        expect(myMockClient.setTCPNoDelay(true)).andReturn(myMockClient);

        replay(mockServer);

        VertxTransport transport = null;
        try {
            transport = new VertxTransport(mockServer, myConfig,
                    new StringEncoderCache(), new StringDecoderCache(),
                    myMockListener, myMockClient);
        }
        catch (final IOException caught) {
            assertThat(caught.getCause(), nullValue());
        }
        finally {
            IOUtils.close(transport);
        }

        verify(mockServer);
    }

    /**
     * Test method for {@link VertxTransport#VertxTransport}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCreateOnThrowInterruptedException() {

        myMockClient = createMock(NetClient.class);
        myMockSocket = createMock(NetSocket.class);
        myMockListener = createMock(TransportResponseListener.class);

        expect(myMockClient.setTCPNoDelay(true)).andReturn(myMockClient);

        expect(
                myMockClient.connect(eq(27017), eq("localhost"),
                        (Handler<AsyncResult<NetSocket>>) anyObject()))
                .andReturn(myMockClient);

        replay();

        VertxTransport transport = null;
        try {
            Thread.currentThread().interrupt();
            transport = new VertxTransport(myServer, myConfig,
                    new StringEncoderCache(), new StringDecoderCache(),
                    myMockListener, myMockClient);
        }
        catch (final IOException caught) {
            assertThat(caught.getCause(),
                    instanceOf(InterruptedException.class));
        }
        finally {
            IOUtils.close(transport);
        }

        verify();
    }

    /**
     * Test method for {@link VertxTransport#VertxTransport}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCreateOnThrowRuntimeException() {
        final RuntimeException toThrow = new RuntimeException("failure");

        myMockClient = createMock(NetClient.class);
        myMockSocket = createMock(NetSocket.class);
        myMockListener = createMock(TransportResponseListener.class);

        expect(myMockClient.setTCPNoDelay(true)).andReturn(myMockClient);

        expect(
                myMockClient.connect(eq(27017), eq("localhost"),
                        (Handler<AsyncResult<NetSocket>>) anyObject()))
                .andThrow(toThrow);

        replay();

        VertxTransport transport = null;
        try {
            transport = new VertxTransport(myServer, myConfig,
                    new StringEncoderCache(), new StringDecoderCache(),
                    myMockListener, myMockClient);
        }
        catch (final IOException caught) {
            assertThat(caught.getCause(), sameInstance((Throwable) toThrow));
        }
        finally {
            IOUtils.close(transport);
        }

        verify();
    }

    /**
     * Test method for {@link VertxTransport#createSendBuffer(int)}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testCreateSendBuffer() throws IOException {

        createConstructorMocks();

        myMockSocket.close();
        expectLastCall();

        replay();

        final VertxTransport transport = new VertxTransport(myServer, myConfig,
                new StringEncoderCache(), new StringDecoderCache(),
                myMockListener, myMockClient);

        final VertxOutputBuffer buffer = transport.createSendBuffer(2345);

        assertThat(buffer.getBuffer().getByteBuf().capacity(), is(2345));

        buffer.close();
        transport.close();

        verify();
    }

    /**
     * Test method for
     * {@link com.allanbank.mongodb.vertx.transport.VertxTransport#flush()}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testFlush() throws IOException {

        createConstructorMocks();

        myMockSocket.close();
        expectLastCall();

        replay();

        final VertxTransport transport = new VertxTransport(myServer, myConfig,
                new StringEncoderCache(), new StringDecoderCache(),
                myMockListener, myMockClient);

        // No effect.
        transport.flush();

        transport.close();

        verify();
    }

    /**
     * Test method for {@link VertxTransport#send(VertxOutputBuffer)}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testSend() throws IOException {
        final VertxOutputBuffer buffer = new VertxOutputBuffer(new Buffer(123),
                new StringEncoderCache());

        createConstructorMocks();

        expect(myMockSocket.write(buffer.getBuffer())).andReturn(myMockSocket);

        myMockSocket.close();
        expectLastCall();

        replay();

        final VertxTransport transport = new VertxTransport(myServer, myConfig,
                new StringEncoderCache(), new StringDecoderCache(),
                myMockListener, myMockClient);

        // No effect.
        transport.send(buffer);

        transport.close();

        verify();
    }

    /**
     * Test method for {@link VertxTransport#start()}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testStart() throws IOException {
        createConstructorMocks();

        myMockSocket.close();
        expectLastCall();

        replay();

        final VertxTransport transport = new VertxTransport(myServer, myConfig,
                new StringEncoderCache(), new StringDecoderCache(),
                myMockListener, myMockClient);

        // No effect.
        transport.start();

        transport.close();

        verify();
    }

    /**
     * Test method for {@link VertxTransport#toString()}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testToString() throws IOException {
        createConstructorMocks();

        expect(myMockSocket.localAddress()).andReturn(
                new InetSocketAddress("localhost", 3245));

        expect(myMockSocket.remoteAddress()).andReturn(
                new InetSocketAddress("localhost", 27017));

        myMockSocket.close();
        expectLastCall();

        replay();

        final VertxTransport transport = new VertxTransport(myServer, myConfig,
                new StringEncoderCache(), new StringDecoderCache(),
                myMockListener, myMockClient);

        assertThat(transport.toString(),
                is("MongoDB(3245-->localhost/127.0.0.1:27017)"));

        transport.close();

        verify();
    }

    /**
     * Setup the mocks for creating a {@link VertxTransport}.
     */
    @SuppressWarnings("unchecked")
    private void createConstructorMocks() {
        myMockClient = createMock(NetClient.class);
        myMockSocket = createMock(NetSocket.class);
        myMockListener = createMock(TransportResponseListener.class);

        expect(myMockClient.setTCPNoDelay(true)).andReturn(myMockClient);
        expect(
                myMockClient.connect(eq(27017), eq("localhost"),
                        handler(myMockSocket))).andReturn(myMockClient);

        expect(myMockSocket.closeHandler(anyObject(VoidHandler.class)))
                .andReturn(myMockSocket);
        expect(myMockSocket.dataHandler(anyObject(Handler.class))).andReturn(
                myMockSocket);
        expect(myMockSocket.exceptionHandler(anyObject(Handler.class)))
                .andReturn(myMockSocket);
    }

    /**
     * Registers a {@link EasyMock} matcher to match the handler argument and
     * provide the result as part of the match.
     *
     * @param exception
     *            The error.
     * @return <code>null</code>.
     */
    private Handler<AsyncResult<NetSocket>> handlerSocket(
            final Throwable exception) {
        handler(exception);
        return null;
    }

    /**
     * Replays the mocks.
     *
     * @param mocks
     *            The mocks to replay.
     */
    private void replay(final Object... mocks) {
        EasyMock.replay(myMockClient, myMockSocket, myMockListener);
        EasyMock.replay(mocks);
    }

    /**
     * Verifies the mocks.
     *
     * @param mocks
     *            The mocks to verify.
     */
    private void verify(final Object... mocks) {
        EasyMock.verify(myMockClient, myMockSocket, myMockListener);
        EasyMock.verify(mocks);
    }
}
