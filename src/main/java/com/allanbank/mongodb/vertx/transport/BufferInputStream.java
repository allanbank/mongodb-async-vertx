/*
 * #%L
 * BufferInputStream.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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

import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.vertx.java.core.buffer.Buffer;

/**
 * BufferInputStream provides an {@link InputStream} backed by a series of
 * {@link Buffer Buffers}.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class BufferInputStream extends InputStream {

    /** The buffers to read from. */
    private final BlockingQueue<Buffer> myBuffers;

    /** The current buffer to read from. */
    private Buffer myCurrent;

    /** The current offset in {@link #myCurrent}. */
    private int myOffset;

    /**
     * Creates a new BufferInputStream.
     */
    public BufferInputStream() {
        super();
        myBuffers = new LinkedBlockingQueue<>();
        myCurrent = null;
        myOffset = 0;
    }

    /**
     * Adds the buffer to the input stream.
     *
     * @param buffer
     *            The buffer to add.
     */
    public void addBuffer(final Buffer buffer) {
        myBuffers.add(buffer);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the number of remaining bytes in the buffers.
     * </p>
     */
    @Override
    public int available() {
        int available = (myCurrent != null) ? myCurrent.length() - myOffset : 0;
        for (final Buffer b : myBuffers) {
            available += b.length();
        }
        return available;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to release all buffers held by the stream.
     * </p>
     */
    @Override
    public void close() {
        myBuffers.clear();
        myCurrent = null;
        myOffset = 0;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the next byte from the next buffer.
     * </p>
     */
    @Override
    public int read() {
        checkAdvanceBuffer();
        if (myCurrent == null) {
            return -1;
        }

        final int offset = myOffset;
        myOffset += 1;

        return myCurrent.getByte(offset);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do a bulk read into the provided buffer.
     * </p>
     */
    @Override
    public int read(final byte b[], final int off, final int len) {
        if (b == null) {
            throw new NullPointerException();
        }
        else if ((off < 0) || (len < 0) || (len > (b.length - off))) {
            throw new IndexOutOfBoundsException();
        }
        else if (len == 0) {
            return 0;
        }

        checkAdvanceBuffer();
        if (myCurrent == null) {
            return -1;
        }

        final int read = Math.min(myCurrent.length() - myOffset, len);

        myCurrent.getByteBuf().getBytes(myOffset, b, off, read);
        myOffset += read;

        return read;
    }

    /**
     * Advances the current buffer if the current buffer has been exhausted.
     */
    private void checkAdvanceBuffer() {
        // While loop for empty buffers.
        while ((myCurrent == null) || (myCurrent.length() == myOffset)) {
            // Next buffer.
            myCurrent = myBuffers.poll();
            myOffset = 0;
            if (myCurrent == null) {
                // Exhausted the buffers.
                return;
            }
        }
    }
}
