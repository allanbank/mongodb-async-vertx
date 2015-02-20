/*
 * #%L
 * BufferOutputStreamTest.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.vertx.java.core.buffer.Buffer;

/**
 * BufferOutputStreamTest provides tests for the {@link BufferOutputStream}
 * class.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class BufferOutputStreamTest {

    /**
     * Test method for {@link BufferOutputStream#write(byte[])}.
     */
    @Test
    public void testWriteByteArray() {
        final Buffer b = new Buffer();

        final BufferOutputStream out = new BufferOutputStream(b);

        assertThat(b.getBytes(), is(new byte[] {}));

        out.write(new byte[0]);
        assertThat(b.getBytes(), is(new byte[] {}));

        out.write(new byte[] { 1 });
        assertThat(b.getBytes(), is(new byte[] { 1 }));

        out.write(new byte[] { 2, 3 });
        assertThat(b.getBytes(), is(new byte[] { 1, 2, 3 }));

        out.close();

        assertThat(b.getBytes(), is(new byte[] { 1, 2, 3 }));
    }

    /**
     * Test method for {@link BufferOutputStream#write(byte[], int, int)}.
     */
    @Test
    public void testWriteByteArrayIntInt() {
        final Buffer b = new Buffer();

        final BufferOutputStream out = new BufferOutputStream(b);

        assertThat(b.getBytes(), is(new byte[] {}));

        out.write(new byte[0], 0, 0);
        assertThat(b.getBytes(), is(new byte[] {}));

        out.write(new byte[] { 0, 1, 2, 3 }, 1, 1);
        assertThat(b.getBytes(), is(new byte[] { 1 }));

        out.write(new byte[] { 0, 1, 2, 3, 4, 5, 6 }, 2, 2);
        assertThat(b.getBytes(), is(new byte[] { 1, 2, 3 }));

        out.close();

        assertThat(b.getBytes(), is(new byte[] { 1, 2, 3 }));
    }

    /**
     * Test method for {@link BufferOutputStream#write(int)}.
     */
    @Test
    public void testWriteInt() {
        final Buffer b = new Buffer();

        final BufferOutputStream out = new BufferOutputStream(b);

        assertThat(b.getBytes(), is(new byte[] {}));

        out.write(1);
        assertThat(b.getBytes(), is(new byte[] { 1 }));

        out.write(2);
        assertThat(b.getBytes(), is(new byte[] { 1, 2 }));

        out.write(3);
        assertThat(b.getBytes(), is(new byte[] { 1, 2, 3 }));

        out.close();

        assertThat(b.getBytes(), is(new byte[] { 1, 2, 3 }));
    }
}
