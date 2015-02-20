/*
 * #%L
 * VertxCloseHandlerTest.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.easymock.Capture;
import org.junit.Test;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.error.ConnectionLostException;

/**
 * VertxCloseHandlerTest provides tests for the {@link VertxCloseHandler} class.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class VertxCloseHandlerTest {

    /**
     * Test method for {@link VertxCloseHandler#handle(java.lang.Void)}.
     */
    @Test
    public void testHandle() {
        final TransportResponseListener mockListener = createMock(TransportResponseListener.class);

        final Capture<MongoDbException> capture = new Capture<>();
        mockListener.closed(capture(capture));
        expectLastCall();

        replay(mockListener);

        final VertxCloseHandler handler = new VertxCloseHandler(mockListener);
        handler.handle(null);

        verify(mockListener);

        assertThat(capture.getValue(),
                instanceOf(ConnectionLostException.class));
    }
}
