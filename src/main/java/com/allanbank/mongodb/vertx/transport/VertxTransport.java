/*
 * #%L
 * NettyTransport.java - mongodb-async-netty - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2014 - 2015 Allanbank Consulting, Inc.
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

import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.Iterator;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.transport.Transport;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * VertxTransport provides a {@link Transport} representing the connection to
 * the server via a Vertx {@link NetSocket} created from a {@link NetClient}.
 *
 * @copyright 2014-2015, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class VertxTransport implements Transport<VertxOutputBuffer> {

    /** The log for the transport. */
    protected static final Log LOG = LogFactory.getLog(VertxTransport.class);

    /** The cache for encoding strings. */
    private final StringEncoderCache myEncoderCache;

    /** The socket for the Connection to the server. */
    private final NetSocket myNetSocket;

    /**
     * Creates a new NettySocketConnection.
     *
     * @param server
     *            The MongoDB server to connect to.
     * @param config
     *            The configuration for the Connection to the MongoDB server.
     * @param encoderCache
     *            Cache used for encoding strings.
     * @param decoderCache
     *            Cache used for decoding strings.
     * @param responseListener
     *            The listener for the status of the transport/connection.
     * @param netClient
     *            The seeded {@link NetClient} for connecting to the server.
     *
     * @throws IOException
     *             On a failure connecting to the MongoDB server.
     */
    public VertxTransport(final Server server,
            final MongoClientConfiguration config,
            final StringEncoderCache encoderCache,
            final StringDecoderCache decoderCache,
            final TransportResponseListener responseListener,
            final NetClient netClient) throws IOException {

        myEncoderCache = encoderCache;

        // Critical settings for performance/function.
        netClient.setTCPNoDelay(true);

        NetSocket socket = null;
        final Iterator<InetSocketAddress> addrIter = server.getAddresses()
                .iterator();
        IOException error = null;
        while (addrIter.hasNext() && (socket == null)) {
            final InetSocketAddress address = addrIter.next();
            try {
                final VertxWaitingHandler<AsyncResult<NetSocket>> handler = new VertxWaitingHandler<>();
                netClient.connect(address.getPort(), address.getHostString(),
                        handler);
                handler.waitFor();

                final AsyncResult<NetSocket> result = handler.getEvent();
                if (result != null) {
                    if (result.succeeded()) {
                        LOG.debug("Connected to {}.", address);
                        socket = result.result();
                    }
                    else {
                        final Throwable t = result.cause();
                        if (t instanceof IOException) {
                            error = (IOException) t;
                        }
                        else {
                            error = new IOException(
                                    "Error connecting to the server '"
                                            + address + "'.", t);
                        }
                    }
                }
            }
            catch (final InterruptedException e) {
                error = new IOException(
                        "Failed to wait for the connection to complete.", e);
            }
            catch (final RuntimeException e) {
                error = new IOException("Failed to create a connection to '"
                        + address + "'.", e);
            }
        }

        if (socket != null) {
            myNetSocket = socket;

            myNetSocket.closeHandler(new VertxCloseHandler(responseListener));
            myNetSocket.dataHandler(new VertxDataHandler(this, decoderCache,
                    responseListener));
            myNetSocket.exceptionHandler(new VertxExceptionHandler(this,
                    responseListener));
        }
        else if (error != null) {
            throw error;
        }
        else {
            throw new IOException(
                    "Failed to create a connection to the server: "
                            + server.getAddresses());
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the {@link NetSocket} backing this connection.
     * </p>
     */
    @Override
    public void close() {
        myNetSocket.close();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use the channel's {@link ByteBufAllocator} to allocate a
     * buffer of the specified size.
     * </p>
     */
    @Override
    public VertxOutputBuffer createSendBuffer(final int size) {
        return new VertxOutputBuffer(new Buffer(size), myEncoderCache);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing.
     * </p>
     */
    @Override
    public void flush() throws IOException {
        // No flush method.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the buffer to the channel.
     * </p>
     */
    @Override
    public void send(final VertxOutputBuffer buffer) throws IOException,
            InterruptedIOException {
        myNetSocket.write(buffer.getBuffer());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing.
     * </p>
     */
    @Override
    public void start() {
        // Nothing to do.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the socket information.
     * </p>
     */
    @Override
    public String toString() {
        final InetSocketAddress local = myNetSocket.localAddress();

        return "MongoDB(" + local.getPort() + "-->"
                + myNetSocket.remoteAddress() + ")";
    }
}
