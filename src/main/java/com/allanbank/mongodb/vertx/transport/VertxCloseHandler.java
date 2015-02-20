/*
 * #%L
 * VertxCloseHandler.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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
import org.vertx.java.core.net.NetSocket;

import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.error.ConnectionLostException;

/**
 * VertxCloseHandler notifies the {@link TransportResponseListener} when the
 * {@link NetSocket} is closed.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class VertxCloseHandler implements Handler<Void> {

    /** The listener to notify. */
    private final TransportResponseListener myListener;

    /**
     * Creates a new VertxCloseHandler.
     *
     * @param listener
     *            The listener to notify.
     */
    protected VertxCloseHandler(final TransportResponseListener listener) {
        myListener = listener;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to notify the listener that the connection was lost.
     * </p>
     */
    @Override
    public void handle(final Void event) {
        myListener.closed(new ConnectionLostException());
    }
}