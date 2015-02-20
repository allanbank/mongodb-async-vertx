/*
 * #%L
 * VertxWaitingHandler.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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

/**
 * VertxWaitingHandler waits for the handler event to trigger.
 *
 * @param <T>
 *            The type of the handler's event.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class VertxWaitingHandler<T> implements Handler<T> {
    /** The event of the handler. */
    private T myEvent = null;

    /**
     * Returns the event received by the handler, if any.
     *
     * @return The event received by the handler.
     */
    public T getEvent() {
        return myEvent;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to capture the results for the event.
     * </p>
     */
    @Override
    public void handle(final T event) {
        synchronized (this) {
            myEvent = event;
            notifyAll();
        }
    }

    /**
     * Waits for the handler's event.
     *
     * @throws InterruptedException
     *             On a failure to wait due to the thread being interrupted.
     */
    public void waitFor() throws InterruptedException {
        synchronized (this) {
            while (myEvent == null) {
                wait();
            }
        }
    }
}