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

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.json.Json;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.vertx.Converter;
import com.allanbank.mongodb.vertx.MongoPersistor;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.VertxAssert;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.Assume.assumeNoException;
import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

public class MongoTypesTest extends PersistorTestParent {

    @Override
    protected JsonObject getConfig() {
        JsonObject config = super.getConfig();
        config.putBoolean("use_mongo_types", true);
        return config;
    }

    @Test
    public void validDatePersists() throws Exception {
        Date date = new Date();
        insertTypedData(date);
    }

    @Test
    public void validByteArrayPersists() throws Exception {
        byte[] data = new byte[] { 1, 2, 3 };
        insertTypedData(data);
    }

    @Test
    public void validArrayListPersists() throws Exception {
        List data = new ArrayList();
        data.add(1);
        data.add(2);
        data.add(Collections.singletonMap("foo", "bar"));
        data.add(4);
        insertTypedData(data);
    }

    @Test
    public void validEmbeddedDocPersists() throws Exception {
        Map<String, Object> y = Collections.singletonMap("y", (Object) 3);
        Map<String, Object> data = Collections.singletonMap("x", (Object) y);
        insertTypedData(data);
    }

    @Test
    public void regexQueryWorks() throws Exception {
        deleteAll(new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> reply) {
                final String testValue = "{\"testKey\" : \"testValue\"}";

                JsonObject data = new JsonObject().putObject("data",
                        new JsonObject(testValue));

                JsonObject json = createSaveQuery(data);
                final Document dataDb = Converter.convert(data);

                JsonObject matcher = new JsonObject().putObject("data.testKey",
                        new JsonObject().putString("$regex", ".*estValu.*"));

                JsonObject query = createMatcher(matcher);

                eb.send(ADDRESS, json, assertStored(query, dataDb, data));
            }
        });
    }

    @Test
    public void elemMatchQueryWorks() throws Exception {
        deleteAll(new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> reply) {

                List data = new ArrayList();
                data.add(1);
                data.add(2);
                data.add(4);

                final DocumentBuilder testValueDb = BuilderFactory.start();
                testValueDb.add("data", data);
                JsonObject document = Converter.convert(testValueDb);
                JsonObject json = createSaveQuery(document);
                final Document dataDb = Converter.convert(document);

                JsonObject matcher = new JsonObject().putObject("data",
                        new JsonObject().putObject("$elemMatch",
                                new JsonObject().putNumber("$gte", 0)
                                        .putNumber("$lt", 5)));

                JsonObject query = createMatcher(matcher);

                eb.send(ADDRESS, json, assertStored(query, dataDb, data));
            }
        });
    }

    private void insertTypedData(final Object data) {
        deleteAll(new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> reply) {
                final String testValue = "{\"testKey\" : \"testValue\"}";
                final DocumentBuilder testValueDb = BuilderFactory.start(Json
                        .parse(testValue));
                testValueDb.add("data", data);

                JsonObject document = Converter.convert(testValueDb);
                JsonObject save = createSaveQuery(document);
                JsonObject matcher = new JsonObject(testValue);
                JsonObject query = createMatcher(matcher);

                eb.send(ADDRESS, save,
                        assertStored(query, testValueDb.build(), data));
            }
        });
    }

    private JsonObject createSaveQuery(JsonObject document) {
        return new JsonObject()
                .putString("collection", PersistorTestParent.COLLECTION)
                .putString("action", "save").putObject("document", document);
    }

    private JsonObject createMatcher(JsonObject matcher) {
        return new JsonObject()
                .putString("collection", PersistorTestParent.COLLECTION)
                .putString("action", "find").putObject("matcher", matcher);
    }

    private Handler<Message<JsonObject>> assertStored(final JsonObject query,
            final DocumentAssignable sentDbObject, final Object dataSaved) {
        return new Handler<Message<JsonObject>>() {
            public void handle(Message<JsonObject> reply) {
                assertEquals(reply.body().toString(), "ok", reply.body()
                        .getString("status"));

                eb.send(ADDRESS, query, new Handler<Message<JsonObject>>() {
                    public void handle(Message<JsonObject> reply) {
                        assertEquals(reply.body().toString(), "ok", reply
                                .body().getString("status"));
                        JsonArray results = reply.body().getArray("results");

                        if (results.size() > 0) {
                            JsonObject result = results.get(0);
                            result.removeField("_id");

                            Document dbObj = Converter.convert(result);
                            Object storedData = dbObj.get("data");
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
}
