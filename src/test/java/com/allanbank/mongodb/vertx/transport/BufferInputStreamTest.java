/*
 * #%L
 * BufferInputStreamTest.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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

import static java.util.Arrays.copyOfRange;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.vertx.java.core.buffer.Buffer;

/**
 * BufferInputStreamTest provides tests for the {@link BufferInputStream} class.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class BufferInputStreamTest {

    /**
     * Test method for {@link BufferInputStream#read()}.
     */
    @Test
    public void testRead() {
        BufferInputStream in = new BufferInputStream();

        assertThat(in.read(), is(-1));

        in.addBuffer(new Buffer(new byte[] { 0, 1, 2, 3, 4 }));
        in.addBuffer(new Buffer(new byte[] { 5, 6, 7 }));
        in.addBuffer(new Buffer(new byte[] {}));
        in.addBuffer(new Buffer(new byte[] { 8, 9 }));
        in.addBuffer(new Buffer(new byte[] { 10 }));

        for (int i = 0; i < 11; ++i) {
            assertThat(in.read(), is(i));
            assertThat(in.available(), is(10 - i));
        }
        assertThat(in.available(), is(0));
        assertThat(in.read(), is(-1));

        in.addBuffer(new Buffer(new byte[] { 8, 9 }));
        assertThat(in.read(), is(8));
        in.close();
        assertThat(in.read(), is(-1));
    }

    /**
     * Test method for
     * {@link com.allanbank.mongodb.vertx.transport.BufferInputStream#read(byte[], int, int)}
     * .
     */
    @Test
    public void testReadByteArrayIntInt() {
        BufferInputStream in = new BufferInputStream();

        in.addBuffer(new Buffer(new byte[] { 0, 1, 2, 3, 4 }));
        in.addBuffer(new Buffer(new byte[] { 5, 6, 7 }));
        in.addBuffer(new Buffer(new byte[] {}));
        in.addBuffer(new Buffer(new byte[] { 8, 9 }));
        in.addBuffer(new Buffer(new byte[] { 10 }));

        byte[] buffer = new byte[100];

        int read = in.read(buffer, 25, 11);
        assertThat(read, is(5));
        assertThat(in.available(), is(6));
        assertThat(copyOfRange(buffer, 25, 30),
                is(new byte[] { 0, 1, 2, 3, 4 }));

        read = in.read(buffer, 35, 11);
        assertThat(read, is(3));
        assertThat(in.available(), is(3));
        assertThat(copyOfRange(buffer, 35, 38), is(new byte[] { 5, 6, 7 }));

        read = in.read(buffer, 2, 11);
        assertThat(read, is(2));
        assertThat(in.available(), is(1));
        assertThat(copyOfRange(buffer, 2, 4), is(new byte[] { 8, 9 }));

        read = in.read(buffer, 10, 11);
        assertThat(read, is(1));
        assertThat(in.available(), is(0));
        assertThat(copyOfRange(buffer, 10, 11), is(new byte[] { 10 }));

        read = in.read(buffer, 10, 11);
        assertThat(read, is(-1));

        in.close();
    }
}
