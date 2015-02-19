/*
 * #%L
 * VertxWaitingHandlerTest.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.Timer;
import java.util.TimerTask;

import org.junit.Test;

/**
 * VertxWaitingHandlerTest provides tests for the {@link VertxWaitingHandler}
 * class.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class VertxWaitingHandlerTest {

    /**
     * Test method for {@link VertxWaitingHandler#waitFor()}.
     * 
     * @throws InterruptedException
     *             On a failure.
     */
    @Test
    public void testWaitForWhenAlreadyHandled() throws InterruptedException {

        VertxWaitingHandler<Long> handler = new VertxWaitingHandler<Long>();
        assertThat(handler.getEvent(), nullValue());

        handler.handle(Long.valueOf(0));

        assertThat(handler.getEvent(), is(Long.valueOf(0)));

        handler.waitFor();

        assertThat(handler.getEvent(), is(Long.valueOf(0)));
    }

    /**
     * Test method for {@link VertxWaitingHandler#waitFor()}.
     * 
     * @throws InterruptedException
     *             On a failure.
     */
    @Test
    public void testWaitForWhenHandledLater() throws InterruptedException {

        final VertxWaitingHandler<Long> handler = new VertxWaitingHandler<Long>();
        assertThat(handler.getEvent(), nullValue());

        final Timer timer = new Timer();
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                handler.handle(Long.valueOf(0));
                timer.cancel();
            }
        }, 100);

        handler.waitFor();
        timer.cancel();

        assertThat(handler.getEvent(), is(Long.valueOf(0)));
    }

    /**
     * Test method for {@link VertxWaitingHandler#waitFor()}.
     */
    @Test
    public void testWaitForWhenInterrupted() {

        VertxWaitingHandler<Long> handler = new VertxWaitingHandler<Long>();
        assertThat(handler.getEvent(), nullValue());

        try {
            Thread.currentThread().interrupt();
            handler.waitFor();
        }
        catch (InterruptedException good) {
            // Expected.
        }

        handler.handle(Long.valueOf(0));
        assertThat(handler.getEvent(), is(Long.valueOf(0)));
    }

}
