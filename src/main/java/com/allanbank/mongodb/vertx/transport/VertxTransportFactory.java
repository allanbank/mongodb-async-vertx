/*
 * #%L
 * VertxTransportFactory.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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

import java.io.IOException;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.vertx.java.core.Vertx;
import org.vertx.java.core.net.NetClient;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.transport.SslEngineFactory;
import com.allanbank.mongodb.client.transport.TransportFactory;
import com.allanbank.mongodb.client.transport.TransportResponseListener;

/**
 * VertxTransportFactory provides factory to create connections to MongoDB via
 * Vert.x {@link NetClient NetClients}.
 *
 * @copyright 2014-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class VertxTransportFactory implements TransportFactory {

    /** The {@link Vertx} instance. */
    private final Vertx myVertx;

    /**
     * Creates a new VertxTransportFactory.
     *
     * @param vertx
     *            The {@link Vertx} instance.
     */
    public VertxTransportFactory(final Vertx vertx) {
        myVertx = vertx;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create a {@link VertxTransport}.
     * </p>
     */
    @Override
    public VertxTransport createTransport(final Server server,
            final MongoClientConfiguration config,
            final StringEncoderCache encoderCache,
            final StringDecoderCache decoderCache,
            final TransportResponseListener responseListener)
                    throws IOException {

        return new VertxTransport(server, config, encoderCache, decoderCache,
                responseListener, createNetClient(config));
    }

    /**
     * Creates a new {@link NetClient} for creating a Vertx connection to the
     * MongoDB server.
     *
     * @param config
     *            The configuration for the client.
     * @return The {@link NetClient} to create a connection to the server.
     */
    protected NetClient createNetClient(final MongoClientConfiguration config) {

        final NetClient netClient = myVertx.createNetClient();

        // Configured values.
        netClient.setConnectTimeout(config.getConnectTimeout());
        netClient.setTCPKeepAlive(config.isUsingSoKeepalive());

        // Trade memory for performance.
        netClient.setUsePooledBuffers(true);

        final SocketFactory socketFactory = config.getSocketFactory();
        if ((socketFactory instanceof SslEngineFactory)
                || (socketFactory instanceof SSLSocketFactory)) {
            netClient.setSSL(true);
        }

        return netClient;
    }

}
