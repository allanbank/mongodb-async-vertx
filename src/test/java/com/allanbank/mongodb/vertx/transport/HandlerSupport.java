/*
 * #%L
 * HandlerSupport.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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

import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;

/**
 * HandlerSupport provides support for working with {@link Handler}s in
 * EasyMock.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class HandlerSupport {

    /**
     * Registers a {@link EasyMock} matcher to match the handler argument and
     * provide the result as part of the match.
     * 
     * @param <T>
     *            The type of result.
     * @param result
     *            The result.
     * @return <code>null</code>.
     */
    public static <T> Handler<AsyncResult<T>> handler(final T result) {
        EasyMock.reportMatcher(new HandlerMatcher<T>(result));

        return null;
    }

    /**
     * Registers a {@link EasyMock} matcher to match the handler argument and
     * provide the result as part of the match.
     * 
     * @param <T>
     *            The type of result.
     * @param result
     *            The result.
     * @return <code>null</code>.
     */
    public static <T> Handler<AsyncResult<T>> handler(final Throwable result) {
        EasyMock.reportMatcher(new HandlerMatcher<T>(result));

        return null;
    }

    /**
     * Creates a new HandlerSupport.
     */
    private HandlerSupport() {
        // Nothing.
    }

    /**
     * HandlerMatcher provides a matcher that also triggers the handler.
     * 
     * @param <T>
     *            The type of the handler.
     * @api.no This class is <b>NOT</b> part of the drivers API. This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver.
     * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
     */
    /* package */static final class HandlerMatcher<T> implements
            IArgumentMatcher {
        /** The result to provide the handler. */
        private final T myResult;

        /** The error to provide the handler. */
        private final Throwable myError;

        /**
         * Creates a new HandlerMatcher.
         * 
         * @param result
         *            The result to provide the handler.
         */
        /* package */HandlerMatcher(T result) {
            myResult = result;
            myError = null;
        }

        /**
         * Creates a new HandlerMatcher.
         * 
         * @param error
         *            The error to provide the handler.
         */
        /* package */HandlerMatcher(Throwable error) {
            myResult = null;
            myError = error;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to append "&lt;handler&gt;".
         * </p>
         */
        @Override
        public void appendTo(StringBuffer buffer) {
            buffer.append("<handler>");
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to trigger the handler.
         * </p>
         */
        @SuppressWarnings("unchecked")
        @Override
        public boolean matches(Object argument) {
            if (argument instanceof Handler) {
                if (myError == null) {
                    ((Handler<AsyncResult<T>>) argument)
                            .handle(new DefaultFutureResult<T>(myResult));
                }
                else {
                    ((Handler<AsyncResult<T>>) argument)
                            .handle(new DefaultFutureResult<T>(myError));
                }
                return true;
            }
            return false;
        }
    }

}
