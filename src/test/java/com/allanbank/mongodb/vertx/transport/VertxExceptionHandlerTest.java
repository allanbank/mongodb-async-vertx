/*
 * #%L
 * VertxExceptionHandlerTest.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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

/**
 * VertxExceptionHandlerTest provides tests for the
 * {@link VertxExceptionHandler} class.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class VertxExceptionHandlerTest {

    /**
     * Test method for {@link VertxExceptionHandler#handle(java.lang.Throwable)}
     * .
     */
    @Test
    public void testHandle() {
        VertxTransport mockTransport = createMock(VertxTransport.class);
        TransportResponseListener mockListener = createMock(TransportResponseListener.class);

        Capture<MongoDbException> capture = new Capture<MongoDbException>();
        mockListener.closed(capture(capture));
        expectLastCall();

        mockTransport.close();
        expectLastCall();

        replay(mockTransport, mockListener);

        VertxExceptionHandler handler = new VertxExceptionHandler(
                mockTransport, mockListener);
        handler.handle(null);

        verify(mockTransport, mockListener);

        assertThat(capture.getValue(), instanceOf(MongoDbException.class));
    }

}
