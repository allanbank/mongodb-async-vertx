/*
 * #%L
 * BufferOutputStream.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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

import java.io.OutputStream;
import java.util.Arrays;

import org.vertx.java.core.buffer.Buffer;

/**
 * BufferOutputStream provides an {@link OutputStream} implementation that
 * writes the data to the provided {@link Buffer}.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class BufferOutputStream extends OutputStream {

    /** The buffer to write to. */
    private final Buffer myBackingBuffer;

    /**
     * Creates a new BufferOutputStream.
     *
     * @param buffer
     *            The buffer to write to.
     */
    public BufferOutputStream(final Buffer buffer) {
        myBackingBuffer = buffer;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing.
     * </p>
     */
    @Override
    public void close() {
        // Nothing.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the byte to the backing buffer.
     * </p>
     */
    @Override
    public void write(final byte[] bytes) {
        myBackingBuffer.appendBytes(bytes);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the byte to the backing buffer.
     * </p>
     */
    @Override
    public void write(final byte[] bytes, final int offset, final int length) {
        myBackingBuffer.appendBytes(Arrays.copyOfRange(bytes, offset, offset
                + length));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the byte to the backing buffer.
     * </p>
     */
    @Override
    public void write(final int b) {
        myBackingBuffer.appendByte((byte) (b & 0xFF));
    }
}
