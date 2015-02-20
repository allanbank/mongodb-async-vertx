/*
 * Copyright 2013 Red Hat, Inc.
 * Copyright 2015 Allanbank Consulting, Inc.
 *
 * Red Hat and Allanbank Consulting licenses this file to you under the
 * Apache License, version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the
 * License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * @author <a href="http://tfox.org">Tim Fox</a> and others.
 */

package org.vertx.mods.mongo.test.integration.java;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.VertxAssert;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.json.Json;
import com.allanbank.mongodb.vertx.Converter;

public class MongoTypesTest extends PersistorTestParent {

    @Test
    public void elemMatchQueryWorks() throws Exception {
        deleteAll(new Handler<Message<JsonObject>>() {
            @Override
            public void handle(final Message<JsonObject> reply) {

                final List data = new ArrayList();
                data.add(1);
                data.add(2);
                data.add(4);

                final DocumentBuilder testValueDb = BuilderFactory.start();
                testValueDb.add("data", data);
                final JsonObject document = Converter.convert(testValueDb);
                final JsonObject json = createSaveQuery(document);
                final Document dataDb = Converter.convert(document);

                final JsonObject matcher = new JsonObject().putObject("data",
                        new JsonObject().putObject("$elemMatch",
                                new JsonObject().putNumber("$gte", 0)
                                        .putNumber("$lt", 5)));

                final JsonObject query = createMatcher(matcher);

                eb.send(ADDRESS, json, assertStored(query, dataDb, data));
            }
        });
    }

    @Test
    public void regexQueryWorks() throws Exception {
        deleteAll(new Handler<Message<JsonObject>>() {
            @Override
            public void handle(final Message<JsonObject> reply) {
                final String testValue = "{\"testKey\" : \"testValue\"}";

                final JsonObject data = new JsonObject().putObject("data",
                        new JsonObject(testValue));

                final JsonObject json = createSaveQuery(data);
                final Document dataDb = Converter.convert(data);

                final JsonObject matcher = new JsonObject().putObject(
                        "data.testKey",
                        new JsonObject().putString("$regex", ".*estValu.*"));

                final JsonObject query = createMatcher(matcher);

                eb.send(ADDRESS, json, assertStored(query, dataDb, data));
            }
        });
    }

    @Test
    public void validArrayListPersists() throws Exception {
        final List data = new ArrayList();
        data.add(1);
        data.add(2);
        data.add(Collections.singletonMap("foo", "bar"));
        data.add(4);
        insertTypedData(data);
    }

    @Test
    public void validByteArrayPersists() throws Exception {
        final byte[] data = new byte[] { 1, 2, 3 };
        insertTypedData(data);
    }

    @Test
    public void validDatePersists() throws Exception {
        final Date date = new Date();
        insertTypedData(date);
    }

    @Test
    public void validEmbeddedDocPersists() throws Exception {
        final Map<String, Object> y = Collections.singletonMap("y", (Object) 3);
        final Map<String, Object> data = Collections.singletonMap("x",
                (Object) y);
        insertTypedData(data);
    }

    @Override
    protected JsonObject getConfig() {
        final JsonObject config = super.getConfig();
        config.putBoolean("use_mongo_types", true);
        return config;
    }

    private Handler<Message<JsonObject>> assertStored(final JsonObject query,
            final DocumentAssignable sentDbObject, final Object dataSaved) {
        return new Handler<Message<JsonObject>>() {
            @Override
            public void handle(final Message<JsonObject> reply) {
                assertEquals(reply.body().toString(), "ok", reply.body()
                        .getString("status"));

                eb.send(ADDRESS, query, new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(final Message<JsonObject> reply) {
                        assertEquals(reply.body().toString(), "ok", reply
                                .body().getString("status"));
                        final JsonArray results = reply.body().getArray(
                                "results");

                        if (results.size() > 0) {
                            final JsonObject result = results.get(0);
                            result.removeField("_id");

                            final Document dbObj = Converter.convert(result);
                            dbObj.get("data");
                            VertxAssert.assertEquals(sentDbObject, dbObj);
                            testComplete();
                        }
                        else {
                            VertxAssert.fail("Stored object not found in DB");
                        }
                    }
                });
            }
        };
    }

    private JsonObject createMatcher(final JsonObject matcher) {
        return new JsonObject()
                .putString("collection", PersistorTestParent.COLLECTION)
                .putString("action", "find").putObject("matcher", matcher);
    }

    private JsonObject createSaveQuery(final JsonObject document) {
        return new JsonObject()
                .putString("collection", PersistorTestParent.COLLECTION)
                .putString("action", "save").putObject("document", document);
    }

    private void insertTypedData(final Object data) {
        deleteAll(new Handler<Message<JsonObject>>() {
            @Override
            public void handle(final Message<JsonObject> reply) {
                final String testValue = "{\"testKey\" : \"testValue\"}";
                final DocumentBuilder testValueDb = BuilderFactory.start(Json
                        .parse(testValue));
                testValueDb.add("data", data);

                final JsonObject document = Converter.convert(testValueDb);
                final JsonObject save = createSaveQuery(document);
                final JsonObject matcher = new JsonObject(testValue);
                final JsonObject query = createMatcher(matcher);

                eb.send(ADDRESS, save,
                        assertStored(query, testValueDb.build(), data));
            }
        });
    }
}
