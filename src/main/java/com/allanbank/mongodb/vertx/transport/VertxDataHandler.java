/*
 * #%L
 * VertxDataHandler.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;

import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetMore;
import com.allanbank.mongodb.client.message.Header;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.KillCursors;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.Update;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.client.transport.bio.MessageInputBuffer;
import com.allanbank.mongodb.error.ConnectionLostException;

/**
 * VertxDataHandler receives each buffer and constructs the received MongoDB
 * {@link Message messages}.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class VertxDataHandler implements Handler<Buffer> {

    /** The BSON input stream to read messages. */
    private final BsonInputStream myBsonIn;

    /** The active header being read. */
    private volatile Header myCurrentHeader;

    /** The current buffers to read. */
    private final BufferInputStream myInput;

    /** The listener for the received messages. */
    private final TransportResponseListener myListener;

    /** Ensures that only one thread is parsing the buffers at a time. */
    private final Lock myParseLock;

    /** The transport for the handler. */
    private final VertxTransport myTransport;

    /**
     * Creates a new VertxDataHandler.
     *
     * @param transport
     *            The transport for the handler.
     * @param cache
     *            The cache for decoding strings.
     * @param listener
     *            The listener for the received messages.
     */
    public VertxDataHandler(final VertxTransport transport,
            final StringDecoderCache cache,
            final TransportResponseListener listener) {
        myTransport = transport;
        myListener = listener;
        myInput = new BufferInputStream();
        myBsonIn = new BsonInputStream(myInput, cache);
        myParseLock = new ReentrantLock();
        myCurrentHeader = null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to append the buffer to the received data and then try and
     * read the entire message.
     * </p>
     */
    @Override
    public void handle(final Buffer event) {

        // addBuffer is thread safe.
        myInput.addBuffer(event);

        // But not parsing and the NetSocket does not appear to enforce a single
        // access to the handler so we have to use a lock.
        try {
            myParseLock.lock();
            readMessages();
        }
        catch (final IOException ioe) {
            final StreamCorruptedException sce = new StreamCorruptedException(
                    ioe.getMessage());
            sce.initCause(ioe);
            myListener.closed(new ConnectionLostException(sce));

            myTransport.close();
        }
        finally {
            myParseLock.unlock();
        }
    }

    /**
     * Reads all of the messages from the read buffers.
     *
     * @throws IOException
     *             On a failure to read the messages.
     */
    protected void readMessages() throws IOException {
        boolean canRead = true;
        while (canRead) {
            final int available = myBsonIn.available();
            if (myCurrentHeader != null) {
                if ((myCurrentHeader.getLength() - Header.SIZE) <= available) {
                    readMessage();
                    myCurrentHeader = null;
                }
                else {
                    canRead = false;
                }
            }
            else if (Header.SIZE <= available) {
                // Read in the header.
                myCurrentHeader = new Header(myBsonIn);
            }
            else {
                canRead = false;
            }
        }
    }

    /**
     * Reads the message described by the {@link #myCurrentHeader current
     * header}.
     *
     * @throws IOException
     *             On a failure reading the message.
     */
    private void readMessage() throws IOException {
        final Operation op = myCurrentHeader.getOperation();
        if (op == null) {
            // Huh? Dazed and confused
            throw new StreamCorruptedException("Unexpected operation read '"
                    + op + "'.");
        }

        Message message = null;
        switch (op) {
        case REPLY:
            message = new Reply(myCurrentHeader, myBsonIn);
            break;
        case QUERY:
            message = new Query(myCurrentHeader, myBsonIn);
            break;
        case UPDATE:
            message = new Update(myBsonIn);
            break;
        case INSERT:
            message = new Insert(myCurrentHeader, myBsonIn);
            break;
        case GET_MORE:
            message = new GetMore(myBsonIn);
            break;
        case DELETE:
            message = new Delete(myBsonIn);
            break;
        case KILL_CURSORS:
            message = new KillCursors(myBsonIn);
            break;
        }

        myListener.response(new MessageInputBuffer(message));
    }
}
