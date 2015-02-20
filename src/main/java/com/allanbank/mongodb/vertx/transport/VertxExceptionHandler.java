/*
 * #%L
 * VertxExceptionHandler.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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

import org.vertx.java.core.Handler;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.transport.TransportResponseListener;

/**
 * VertxExceptionHandler provides the {@link Handler} for exceptions encountered
 * by the transport.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class VertxExceptionHandler implements Handler<Throwable> {

    /** The listener to notify. */
    private final TransportResponseListener myListener;

    /** The transport to close on an error. */
    private final VertxTransport myTransport;

    /**
     * Creates a new VertxExceptionHandler.
     *
     * @param transport
     *            The transport to close on an error.
     * @param listener
     *            The listener to notify.
     */
    protected VertxExceptionHandler(final VertxTransport transport,
            final TransportResponseListener listener) {
        myListener = listener;
        myTransport = transport;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to notify the driver of the exception and to close the
     * transport.
     * </p>
     */
    @Override
    public void handle(final Throwable event) {
        myListener.closed(new MongoDbException(event));
        myTransport.close();
    }
}