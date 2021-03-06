/*
 * #%L
 * VertxDataHandlerTest.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.easymock.Capture;
import org.easymock.CaptureType;
import org.junit.Test;
import org.vertx.java.core.buffer.Buffer;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetMore;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.KillCursors;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.Update;
import com.allanbank.mongodb.client.transport.TransportInputBuffer;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.client.transport.bio.MessageInputBuffer;
import com.allanbank.mongodb.error.ConnectionLostException;

/**
 * VertxDataHandlerTest provides tests for the {@link VertxDataHandler} class.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class VertxDataHandlerTest {

    /**
     * Test method for {@link VertxDataHandler#handle(Buffer)}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testHandleBadOpCode() throws IOException {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new KillCursors(new long[] { rand.nextLong() },
                ReadPreference.PRIMARY);
        final int msgId = rand.nextInt() & 0xFFFFFF;

        final VertxTransport mockTransport = createMock(VertxTransport.class);
        final TransportResponseListener mockListener = createMock(TransportResponseListener.class);

        final Capture<MongoDbException> captureMsg = new Capture<>();
        mockListener.closed(capture(captureMsg));
        expectLastCall();

        mockTransport.close();
        expectLastCall();

        replay(mockTransport, mockListener);

        final VertxDataHandler handler = new VertxDataHandler(mockTransport,
                new StringDecoderCache(), mockListener);

        final byte[] bytes = createBytes(msgId, msg);

        // OpCode is bytes 12-16.
        bytes[12] = (byte) 0xFF;
        Arrays.fill(bytes, 12, 16, (byte) 0xFF);

        for (final Buffer b : createBuffers(rand, bytes)) {
            handler.handle(b);
        }

        verify(mockTransport, mockListener);

        assertThat(captureMsg.getValue(),
                instanceOf(ConnectionLostException.class));
    }

    /**
     * Test method for {@link VertxDataHandler#handle(Buffer)}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testHandleDelete() throws IOException {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new Delete("db", "c", Find.ALL, rand.nextBoolean());

        runHandle(rand, msg);
    }

    /**
     * Test method for {@link VertxDataHandler#handle(Buffer)}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testHandleGetMore() throws IOException {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new GetMore("db", "c", rand.nextLong(),
                rand.nextInt(), ReadPreference.PRIMARY);

        runHandle(rand, msg);
    }

    /**
     * Test method for {@link VertxDataHandler#handle(Buffer)}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testHandleInsert() throws IOException {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new Insert("db", "c",
                Collections.singletonList(Find.ALL), rand.nextBoolean());

        runHandle(rand, msg);
    }

    /**
     * Test method for {@link VertxDataHandler#handle(Buffer)}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testHandleKillCursors() throws IOException {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new KillCursors(new long[] { rand.nextLong() },
                ReadPreference.PRIMARY);

        runHandle(rand, msg);
    }

    /**
     * Test method for {@link VertxDataHandler#handle(Buffer)}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testHandleManyMessages() throws IOException {
        final Random rand = new Random(System.currentTimeMillis());
        final int msgId = rand.nextInt() & 0xFFFFFF;
        final int iterations = 25 + rand.nextInt(150);
        final Message msg = new Update("db", "collection", Find.ALL, Find.ALL,
                true, false);

        // Create some random sized buffers to read.
        final List<Buffer> buffers = new ArrayList<>();
        for (int i = 0; i < iterations; ++i) {
            buffers.addAll(createBuffers(rand, createBytes(msgId, msg)));

            // Randomly merge two buffers.
            if (buffers.size() > 5) {
                final int index = rand.nextInt(buffers.size() - 1);

                final Buffer first = buffers.get(index);
                final Buffer second = buffers.remove(index + 1);

                first.appendBuffer(second);
            }
        }

        final VertxTransport mockTransport = createMock(VertxTransport.class);
        final TransportResponseListener mockListener = createMock(TransportResponseListener.class);

        final Capture<TransportInputBuffer> captureMsg = new Capture<>(
                CaptureType.ALL);
        mockListener.response(capture(captureMsg));
        expectLastCall().times(iterations);

        replay(mockTransport, mockListener);

        final VertxDataHandler handler = new VertxDataHandler(mockTransport,
                new StringDecoderCache(), mockListener);

        for (final Buffer b : buffers) {
            handler.handle(b);
        }

        verify(mockTransport, mockListener);

        assertThat(captureMsg.getValues(), hasSize(iterations));
        assertThat(captureMsg.getValues().get(0),
                instanceOf(MessageInputBuffer.class));
        assertThat(captureMsg.getValues().get(0).read(), is(msg));
    }

    /**
     * Test method for {@link VertxDataHandler#handle(Buffer)}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testHandleQuery() throws IOException {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new Query("db", "c", Find.ALL, null, 0, 0,
                rand.nextInt() & 0xFFFFFF, rand.nextBoolean(),
                ReadPreference.PRIMARY, rand.nextBoolean(), rand.nextBoolean(),
                rand.nextBoolean(), rand.nextBoolean());

        runHandle(rand, msg);
    }

    /**
     * Test method for {@link VertxDataHandler#handle(Buffer)}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testHandleReply() throws IOException {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new Reply(rand.nextInt() & 0xFFFFFF,
                rand.nextLong(), rand.nextInt(),
                Collections.singletonList(Find.ALL), rand.nextBoolean(),
                rand.nextBoolean(), rand.nextBoolean(), rand.nextBoolean());

        runHandle(rand, msg);
    }

    /**
     * Test method for {@link VertxDataHandler#handle(Buffer)}.
     *
     * @throws IOException
     *             On a failure.
     */
    @Test
    public void testHandleUpdate() throws IOException {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new Update("db", "collection", Find.ALL, Find.ALL,
                true, false);

        runHandle(rand, msg);
    }

    /**
     * Performs a testing using the random and message as the data.
     *
     * @param rand
     *            The source of randomness.
     * @param msg
     *            The message to receive.
     * @throws IOException
     *             On a failure.
     */
    protected void runHandle(final Random rand, final Message msg)
            throws IOException {
        final int msgId = rand.nextInt() & 0xFFFFFF;

        final VertxTransport mockTransport = createMock(VertxTransport.class);
        final TransportResponseListener mockListener = createMock(TransportResponseListener.class);

        final Capture<TransportInputBuffer> captureMsg = new Capture<>();
        mockListener.response(capture(captureMsg));
        expectLastCall();

        replay(mockTransport, mockListener);

        final VertxDataHandler handler = new VertxDataHandler(mockTransport,
                new StringDecoderCache(), mockListener);

        for (final Buffer b : createBuffers(rand, createBytes(msgId, msg))) {
            handler.handle(b);
        }

        verify(mockTransport, mockListener);

        assertThat(captureMsg.getValue(), instanceOf(MessageInputBuffer.class));
        assertThat(captureMsg.getValue().read(), is(msg));
    }

    /**
     * Breaks up the bytes into random sized {@link Buffer}s.
     *
     * @param random
     *            The source of randomness.
     * @param bytes
     *            The bytes to put into buffers.
     * @return The buffers containing the bytes.
     */
    private List<Buffer> createBuffers(final Random random, final byte[] bytes) {
        if (random.nextBoolean() || (bytes.length <= 1)) {
            return Collections.singletonList(new Buffer(bytes));
        }

        final List<Buffer> buffers = new ArrayList<>();
        final int split = random.nextInt(bytes.length);

        buffers.add(new Buffer(Arrays.copyOf(bytes, split)));
        buffers.addAll(createBuffers(random,
                Arrays.copyOfRange(bytes, split, bytes.length)));

        return buffers;
    }

    /**
     * Converts the message into bytes.
     *
     * @param id
     *            The id for the message.
     * @param msg
     *            The message.
     * @return The bytes for the message.
     * @throws IOException
     *             On a failure writing the message.
     */
    private byte[] createBytes(final int id, final Message msg)
            throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BsonOutputStream bout = new BsonOutputStream(out);

        msg.write(id, bout);

        return out.toByteArray();
    }
}
