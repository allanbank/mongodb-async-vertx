/*
 * #%L
 * NettyOutputBuffer.java - mongodb-async-netty - Allanbank Consulting, Inc.
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

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;

import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.transport.TransportOutputBuffer;

/**
 * VertxOutputBuffer provides the buffer for serializing messages to send via
 * the Vert.x {@link NetSocket}.
 *
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class VertxOutputBuffer implements TransportOutputBuffer {

    /** The {@link Buffer} baking this buffer. */
    private final Buffer myBackingBuffer;

    /** The BSON output stream wrapping the ByteBuf. */
    private final BsonOutputStream myBsonOut;

    /**
     * Creates a new NettyOutputBuffer.
     *
     * @param buffer
     *            The backing {@link ByteBuf}.
     * @param cache
     *            The cache for encoding strings.
     */
    public VertxOutputBuffer(final Buffer buffer, final StringEncoderCache cache) {

        myBackingBuffer = buffer;
        myBsonOut = new BsonOutputStream(new BufferOutputStream(buffer), cache);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing.
     * </p>
     */
    @Override
    public void close() {
        // Nothing;
    }

    /**
     * Returns the backing {@link Buffer}.
     *
     * @return The backing {@link Buffer}.
     */
    public Buffer getBuffer() {
        return myBackingBuffer;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the message to the backing {@link ByteBuf}.
     * </p>
     */
    @Override
    public void write(final int messageId, final Message message,
            final ReplyCallback callback) throws IOException {

        message.write(messageId, myBsonOut);
    }
}
