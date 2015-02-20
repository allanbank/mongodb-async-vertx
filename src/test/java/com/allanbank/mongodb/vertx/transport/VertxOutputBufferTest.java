/*
 * #%L
 * VertxOutputBufferTest.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import org.junit.Test;
import org.vertx.java.core.buffer.Buffer;

import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.ServerStatus;

/**
 * VertxOutputBufferTest provides tests for the {@link VertxOutputBuffer} class.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class VertxOutputBufferTest {

    /**
     * Test method for {@link VertxOutputBuffer#write}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testWrite() throws IOException {
        final Random rand = new Random(System.currentTimeMillis());
        final int msgId = rand.nextInt() & 0xFFFFFF;
        final Message msg = new ServerStatus();

        final ByteArrayOutputStream o = new ByteArrayOutputStream();
        final BsonOutputStream bout = new BsonOutputStream(o);
        msg.write(msgId, bout);

        final Buffer buffer = new Buffer();
        final VertxOutputBuffer out = new VertxOutputBuffer(buffer,
                new StringEncoderCache());
        out.write(msgId, msg, null);

        assertThat(out.getBuffer(), sameInstance(buffer));
        assertThat(buffer.getBytes(), is(o.toByteArray()));

        out.close();

        // Close does not effect the buffer.
        assertThat(out.getBuffer(), sameInstance(buffer));
        assertThat(buffer.getBytes(), is(o.toByteArray()));
    }
}
