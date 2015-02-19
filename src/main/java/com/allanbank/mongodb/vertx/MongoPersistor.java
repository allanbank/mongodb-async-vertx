/*
 * #%L
 * MongoPersistor.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.vertx;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static com.allanbank.mongodb.vertx.Converter.convert;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocketFactory;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.DurabilityEditor;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.MongoStreamCursorControl;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.ReadPreferenceEditor;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.json.Json;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.FindAndModify;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;
import com.allanbank.mongodb.vertx.transport.VertxTransportFactory;

/**
 * MongoPersistor provides the interface to MongoDB via the Vert.x Bus.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoPersistor extends BusModBase implements
        Handler<Message<JsonObject>> {

    /** The log for the persistor. */
    protected static final Log LOG = LogFactory.getLog(MongoPersistor.class);

    /** The address for the MongoDB service on the Vert.x bus. */
    protected String myAddress;

    /** The database for the service. */
    protected MongoDatabase myDatabase;

    /** The name of the database to use. */
    protected String myDatabaseName;

    /** The durability for writes. */
    protected Durability myDurability = null;

    /** The host for the MongoDB server. */
    protected String myHost;

    /** The client connected to the server. */
    protected MongoClient myMongoClient;

    /** The password to use when authenticating to the MongoDB server. */
    protected String myPassword;

    /** The port for the MongoDB server. */
    protected int myPort;

    /** The default read preference to use. */
    protected ReadPreference myReadPreference;

    /** The read and connect timeout. */
    protected int mySocketTimeout;

    /** The user name to use when authenticating to the MongoDB server. */
    protected String myUsername;

    /** If true then use a TLS socket factory. */
    protected boolean myUseTls;

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to handle a message from the bus.
     * </p>
     */
    @Override
    public void handle(Message<JsonObject> message) {
        String action = message.body().getString("action");
        LOG.debug("{}: Received {} request.", message.hashCode(), action);
        if (action == null) {
            sendError(message, "action must be specified");
            return;
        }

        try {

            // Note actions should not be in camel case, but should use
            // underscores. I have kept this version with camel case so as not
            // to break compatibility

            if ("save".equalsIgnoreCase(action)) {
                doSave(message);
            }
            else if ("update".equalsIgnoreCase(action)) {
                doUpdate(message);
            }
            else if ("find".equalsIgnoreCase(action)) {
                doFind(message);
            }
            else if ("findone".equalsIgnoreCase(action)) {
                doFindOne(message);
            }
            else if ("find_and_modify".equalsIgnoreCase(action)) {
                // no need for a backwards compatible "findAndModify" since this
                // feature was added after
                doFindAndModify(message);
            }
            else if ("delete".equalsIgnoreCase(action)) {
                doDelete(message);
            }
            else if ("count".equalsIgnoreCase(action)) {
                doCount(message);
            }
            else if ("get_collections".equalsIgnoreCase(action)
                    || "getCollections".equalsIgnoreCase(action)) {
                doListCollectionNames(message);
            }
            else if ("drop_collection".equalsIgnoreCase(action)
                    || "dropCollection".equalsIgnoreCase(action)) {
                doDropCollection(message);
            }
            else if ("collection_stats".equalsIgnoreCase(action)
                    || "collectionStats".equalsIgnoreCase(action)) {
                doCollectionStats(message);
            }
            else if ("aggregate".equalsIgnoreCase(action)) {
                doAggregation(message);
            }
            else if ("command".equalsIgnoreCase(action)) {
                doRunCommand(message);
            }
            else {
                sendError(message, "Invalid action: " + action);
            }
        }
        catch (MongoDbException e) {
            sendError(message, e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to start the persistor.
     * </p>
     */
    @Override
    public void start() {
        super.start();

        myAddress = getOptionalStringConfig("address", "vertx.mongopersistor");

        myHost = getOptionalStringConfig("host", "localhost");
        myPort = getOptionalIntConfig("port", 27017);
        myDatabaseName = getOptionalStringConfig("db_name", "default_db");
        myUsername = getOptionalStringConfig("username", null);
        myPassword = getOptionalStringConfig("password", null);

        ReadPreferenceEditor editor = new ReadPreferenceEditor();
        editor.setAsText(getOptionalStringConfig("read_preference", "primary"));
        myReadPreference = (ReadPreference) editor.getValue();

        int poolSize = getOptionalIntConfig("pool_size", 10);
        mySocketTimeout = getOptionalIntConfig("socket_timeout", 60000);
        myUseTls = getOptionalBooleanConfig("use_ssl", false);

        JsonArray seedsProperty = config.getArray("seeds");

        MongoClientConfiguration mongodbConfig = new MongoClientConfiguration();
        mongodbConfig
                .setTransportFactory(new VertxTransportFactory(getVertx()));
        mongodbConfig.setMaxConnectionCount(poolSize);
        mongodbConfig.setReadTimeout(mySocketTimeout);
        mongodbConfig.setConnectTimeout(mySocketTimeout);
        mongodbConfig.setDefaultReadPreference(myReadPreference);

        if (myUseTls) {
            mongodbConfig.setSocketFactory(SSLSocketFactory.getDefault());
        }

        if (seedsProperty == null) {
            mongodbConfig.addServer(new InetSocketAddress(myHost, myPort));
        }
        else {
            makeSeeds(seedsProperty, mongodbConfig);
        }

        myMongoClient = MongoFactory.createClient(mongodbConfig);
        myDatabase = myMongoClient.getDatabase(myDatabaseName);
        if (myUsername != null && myPassword != null) {
            mongodbConfig.addCredential(Credential.builder()
                    .userName(myUsername).password(myPassword.toCharArray()));
        }

        String durabilityString = getOptionalStringConfig("writeConcern",
                getOptionalStringConfig("write_concern", ""));
        if (!durabilityString.isEmpty()) {
            DurabilityEditor durabilityEditor = new DurabilityEditor();
            durabilityEditor.setAsText(durabilityString);
            myDurability = (Durability) durabilityEditor.getValue();
        }

        if (myDurability == null) {
            myDurability = mongodbConfig.getDefaultDurability();
        }

        eb.registerHandler(myAddress, this);

        LOG.info("Started the MongoPersistor for database '{}' @ {}.",
                myDatabaseName, myAddress);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to stop the persistor. Closes the {@link MongoClient}.
     * </p>
     */
    @Override
    public void stop() {
        IOUtils.close(myMongoClient);
        super.stop();
        LOG.info("Stopped the MongoPersistor for database '{}' @ {}.",
                myDatabaseName, myAddress);
    }

    /**
     * Performs a {@link MongoCollection#stream(StreamCallback, Aggregate)}.
     * 
     * @param message
     *            The message with the details of the find.
     */
    protected void doAggregation(Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            sendError(message, "'collection' is missing for 'aggregation'.");
            return;
        }
        MongoCollection coll = myDatabase.getCollection(collection);

        JsonArray pipelines = message.body().getArray("pipelines");
        if (isPipelinesMissing(pipelines)) {
            sendError(message, "'pipelines' is missing for 'aggregation'.");
            return;
        }

        Aggregate.Builder aggregate = Aggregate.builder();
        // aggregate.useCursor();
        for (int i = 0; i < pipelines.size(); ++i) {
            JsonObject stage = pipelines.get(i);

            aggregate.step(convert(stage));
        }

        Integer batchSize = (Integer) message.body().getNumber("batch_size");
        if (batchSize == null) {
            batchSize = Integer.MAX_VALUE;
        }
        else {
            aggregate.batchSize(batchSize);
        }

        Integer timeoutMS = (Integer) message.body().getNumber("timeout");
        if ((timeoutMS != null) && (0 < timeoutMS)) {
            aggregate.maximumTime(timeoutMS, TimeUnit.MILLISECONDS);
        }
        else {
            timeoutMS = 10000; // 10 seconds but only for the vertx messages.
        }

        SimpleBatchingCallback callback = new SimpleBatchingCallback(message,
                batchSize, timeoutMS);
        MongoStreamCursorControl control = coll.stream(callback, aggregate);
        callback.setCursor(control);
    }

    /**
     * Performs the {@code collection_stats} action.
     * 
     * @param message
     *            The original message.
     */
    protected void doCollectionStats(Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            sendError(message,
                    "'collection' is missing for 'collection_stats'.");
            return;
        }

        // There is no async version of #stats() so run the command instead.
        Document statsCommand = d(e("collStats", collection)).build();
        myDatabase.runCommandAsync(new DocumentCallback("stats", message),
                statsCommand);
    }

    /**
     * Performs a
     * {@link MongoCollection#countAsync(Callback, com.allanbank.mongodb.bson.DocumentAssignable)}
     * .
     * 
     * @param message
     *            The message with the details of the find.
     */
    protected void doCount(Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            sendError(message, "'collection' is missing for 'count'.");
            return;
        }
        MongoCollection coll = myDatabase.getCollection(collection);

        JsonObject matcher = message.body().getObject("matcher");
        if (matcher != null) {
            coll.countAsync(new ResultCallback<Long>("count", message));
        }
        else {
            coll.countAsync(new ResultCallback<Long>("count", message),
                    convert(matcher));
        }
    }

    /**
     * Performs a
     * {@link MongoCollection#deleteAsync(Callback, com.allanbank.mongodb.bson.DocumentAssignable, boolean, Durability)}
     * .
     * 
     * @param message
     *            The message with the details of the find.
     */
    protected void doDelete(Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            sendError(message, "'collection' is missing for 'delete'.");
            return;
        }
        MongoCollection coll = myDatabase.getCollection(collection);

        JsonObject matcher = getMandatoryObject("matcher", message);
        if (matcher == null) {
            sendError(message, "'matcher' is missing for 'delete'.");
            return;
        }

        coll.deleteAsync(new ResultCallback<Long>(message), convert(matcher),
                false, myDurability);
    }

    /**
     * Performs the {@code drop_collection} action.
     * 
     * @param message
     *            The original message.
     */
    protected void doDropCollection(Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            sendError(message, "'collection' is missing for 'drop_collection'.");
            return;
        }

        // No async version of #drop() so manually run the command.
        Document dropCommand = d(e("drop", collection)).build();
        myDatabase.runCommandAsync(new DocumentCallback(message), dropCommand);
    }

    /**
     * Performs a {@link MongoCollection#findAsync(Callback, Find)}.
     * 
     * @param message
     *            The message with the details of the find.
     */
    protected void doFind(Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            sendError(message, "'collection' is missing for 'find'.");
            return;
        }
        MongoCollection coll = myDatabase.getCollection(collection);

        Find.Builder find = Find.builder();

        JsonObject matcher = message.body().getObject("matcher");
        if (matcher != null) {
            find.query(convert(matcher));
        }
        else {
            find.query(Find.ALL);
        }

        JsonObject keys = message.body().getObject("keys");
        if (keys != null) {
            find.projection(convert(keys));
        }

        Integer limit = (Integer) message.body().getNumber("limit");
        if (limit != null) {
            find.limit(limit);
        }

        Integer skip = (Integer) message.body().getNumber("skip");
        if (skip != null) {
            find.skip(skip);
        }

        Integer batchSize = (Integer) message.body().getNumber("batch_size");
        if (batchSize == null) {
            batchSize = Integer.MAX_VALUE;
        }
        else {
            find.batchSize(batchSize);
        }

        Integer timeout = (Integer) message.body().getNumber("timeout");
        if ((timeout != null) && (0 < timeout)) {
            find.maximumTime(timeout, TimeUnit.MILLISECONDS);
        }
        else {
            timeout = 10000; // 10 seconds but only for the vertx messages.
        }

        Object hint = message.body().getField("hint");
        if (hint != null) {
            if (hint instanceof JsonObject) {
                find.hint(convert((JsonObject) hint));
            }
            else if (hint instanceof String) {
                find.hint((String) hint);
            }
            else {
                throw new IllegalArgumentException("Cannot handle type "
                        + hint.getClass().getSimpleName());
            }
        }

        Object sort = message.body().getField("sort");
        if (sort != null) {
            find.sort(convertSort(sort));
        }

        SimpleBatchingCallback callback = new SimpleBatchingCallback(message,
                batchSize, timeout);

        MongoStreamCursorControl cursor = coll.stream(callback, find);
        callback.setCursor(cursor);
    }

    /**
     * Performs a
     * {@link MongoCollection#findAndModifyAsync(Callback, FindAndModify)}.
     * 
     * @param message
     *            The message with the details of the find.
     */
    protected void doFindAndModify(Message<JsonObject> message) {
        final JsonObject msgBody = message.body();

        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            sendError(message, "'collection' is missing for 'find_and_modify'.");
            return;
        }
        MongoCollection coll = myDatabase.getCollection(collection);

        FindAndModify.Builder findAndModify = FindAndModify.builder();

        Document query = convertNullSafe(msgBody.getObject("matcher"));
        if (query != null) {
            findAndModify.query(query);
        }

        Document update = convertNullSafe(msgBody.getObject("update"));
        if (update != null) {
            findAndModify.update(update);
        }

        Document sort = convertNullSafe(msgBody.getObject("sort"));
        if (sort != null) {
            findAndModify.sort(sort);
        }

        Document fields = convertNullSafe(msgBody.getObject("fields"));
        if (fields != null) {
            findAndModify.fields(fields);
        }

        findAndModify.remove(msgBody.getBoolean("remove", false));
        findAndModify.returnNew(msgBody.getBoolean("new", false));
        findAndModify.upsert(msgBody.getBoolean("upsert", false));

        coll.findAndModifyAsync(new DocumentCallback(message), findAndModify);
    }

    /**
     * Performs a {@link MongoCollection#findOneAsync(Callback, Find)}.
     * 
     * @param message
     *            The message with the details of the findOne.
     */
    protected void doFindOne(Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            sendError(message, "'collection' is missing for 'findone'.");
            return;
        }
        MongoCollection coll = myDatabase.getCollection(collection);

        Find.Builder find = Find.builder();

        JsonObject matcher = message.body().getObject("matcher");
        find.query((matcher != null) ? convert(matcher) : Find.ALL);

        JsonObject keys = message.body().getObject("keys");
        if (keys != null) {
            find.projection(convert(keys));
        }

        coll.findOneAsync(new DocumentCallback(message), find);
    }

    /**
     * Performs the {@code get_collections} action.
     * <p>
     * TODO - This method is not async.
     * </p>
     * 
     * @param message
     *            The original message.
     */
    protected void doListCollectionNames(Message<JsonObject> message) {
        JsonObject reply = new JsonObject();
        reply.putArray("collections", new JsonArray(myDatabase
                .listCollectionNames().toArray()));
        sendOK(message, reply);
    }

    /**
     * Performs the {@code command} action.
     * 
     * @param message
     *            The original message.
     */
    protected void doRunCommand(Message<JsonObject> message) {

        Object command = message.body().getField("command");
        if (command == null) {
            sendError(message, "'command' is missing for 'run_command'.");
            return;
        }

        Document commandDoc;
        if (command instanceof String) {
            commandDoc = Json.parse(command.toString());
        }
        else if (command instanceof JsonObject) {
            commandDoc = convert((JsonObject) command);
        }
        else {
            sendError(message,
                    "'command' cannot be converted for the 'run_command'.");
            return;
        }

        myDatabase.runCommandAsync(new DocumentCallback(message), commandDoc);
    }

    /**
     * Performs the {@code save} action.
     * 
     * @param message
     *            The original message.
     */
    protected void doSave(final Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            sendError(message, "'collection' is missing for 'save'.");
            return;
        }
        MongoCollection coll = myDatabase.getCollection(collection);

        JsonObject doc = getMandatoryObject("document", message);
        if (doc == null) {
            sendError(message, "'document' is missing for 'save'.");
            return;
        }

        final JsonObject reply;
        if (doc.getField("_id") == null) {
            String genID = UUID.randomUUID().toString();

            doc.putString("_id", genID);

            reply = new JsonObject();
            reply.putString("_id", genID);
        }
        else {
            reply = null;
        }

        coll.saveAsync(new ResultCallback<Integer>(reply, message),
                convert(doc), myDurability);
    }

    /**
     * Performs the {@code update} action.
     * 
     * @param message
     *            The original message.
     */
    protected void doUpdate(Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            sendError(message, "'collection' is missing for 'update'.");
            return;
        }
        MongoCollection coll = myDatabase.getCollection(collection);

        JsonObject criteriaJson = getMandatoryObject("criteria", message);
        if (criteriaJson == null) {
            sendError(message, "'criteria' is missing for 'update'.");
            return;
        }
        Document criteria = convert(criteriaJson);

        JsonObject objNewJson = getMandatoryObject("objNew", message);
        if (objNewJson == null) {
            return;
        }
        Document objNew = convert(objNewJson);

        Boolean upsert = message.body().getBoolean("upsert", false);
        Boolean multi = message.body().getBoolean("multi", false);

        coll.updateAsync(new ResultCallback<Long>(message), criteria, objNew,
                multi, upsert, myDurability);
    }

    /**
     * Replies with an error.
     * 
     * @param message
     *            The original message.
     * @param thrown
     *            The exception causing the error.
     */
    protected void sendError(Message<JsonObject> message, Throwable thrown) {
        LOG.debug("{}: Sending error reply.", message.hashCode());

        if (thrown instanceof Exception) {
            sendError(message, thrown.getMessage(), (Exception) thrown);
        }
        else {
            sendError(message, thrown.getMessage(), new Exception(thrown));
        }
    }

    /**
     * Replies with an optional reply.
     * 
     * @param message
     *            The original message.
     * @param reply
     *            The optional reply.
     */
    @Override
    protected void sendOK(Message<JsonObject> message, JsonObject reply) {
        LOG.debug("{}: Sending OK reply.", message.hashCode());
        if (reply != null) {
            super.sendOK(message, reply);
        }
        else {
            super.sendOK(message);
        }
    }

    /**
     * Converts the JsonObject to a Document while checking for null values.
     * 
     * @param object
     *            The {@link JsonObject}. May be <code>null</code>.
     * @return If {@code object} is not null then the converted document. If
     *         {@code object} is null then null.
     */
    private Document convertNullSafe(JsonObject object) {
        if (object != null) {
            return convert(object);
        }
        return null;
    }

    /**
     * Converts the sort specification object or array into a sort document.
     * 
     * @param sortObj
     *            The sort specification.
     * @return The sort document.
     */
    private Document convertSort(Object sortObj) {
        if (sortObj instanceof JsonObject) {
            // Backwards compatability and a simpler syntax for single-property
            // sorting
            return convert((JsonObject) sortObj);
        }
        else if (sortObj instanceof JsonArray) {
            JsonArray sortJsonObjects = (JsonArray) sortObj;

            DocumentBuilder docBuilder = BuilderFactory.start();
            for (Object curSortObj : sortJsonObjects) {
                if (!(curSortObj instanceof JsonObject)) {
                    throw new IllegalArgumentException("Cannot handle type "
                            + curSortObj.getClass().getSimpleName());
                }

                for (Element element : convert((JsonObject) curSortObj)) {
                    docBuilder.add(element);
                }
            }

            return docBuilder.build();
        }
        else {
            throw new IllegalArgumentException("Cannot handle type "
                    + sortObj.getClass().getSimpleName());
        }
    }

    /**
     * Makes sure that the pipeline has at least one element.
     * 
     * @param pipelines
     *            The pipeline elements.
     * @return True if the pipeline has at least one element.
     */
    private boolean isPipelinesMissing(JsonArray pipelines) {
        return pipelines == null || pipelines.size() == 0;
    }

    /**
     * Populates the configuration with the seed servers.
     * 
     * @param seedsProperty
     *            The {@link JsonArray} with the seed addresses.
     * @param mongodbConfig
     *            The configuration to populate.
     */
    private void makeSeeds(JsonArray seedsProperty,
            MongoClientConfiguration mongodbConfig) {
        for (Object elem : seedsProperty) {
            JsonObject address = (JsonObject) elem;
            String host = address.getString("host");
            int port = address.getInteger("port");
            mongodbConfig.addServer(new InetSocketAddress(host, port));
        }
    }

    /**
     * SimpleBatchingCallback provides the logic to handle batching results to
     * the client.
     * 
     * @api.no This class is <b>NOT</b> part of the drivers API. This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver.
     * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected final class SimpleBatchingCallback implements
            StreamCallback<Document> {

        /** The control for the cursor. */
        protected MongoStreamCursorControl myCursorControl;

        /** The original message. */
        protected Message<JsonObject> myMessage;

        /** The size for each batch. */
        private final int myBatchSize;

        /** The error for the callback. */
        private Throwable myError;

        /** The pending documents. */
        private final Queue<Document> myPending;

        /** The timeout, in milliseconds, to receive replies. */
        private final int myTimeoutMS;

        /**
         * Creates a new SimpleBatchingCallback.
         * 
         * @param message
         *            The original request message.
         * @param batchSize
         *            The size of messages to batch.
         * @param timeoutMS
         *            The number of milliseconds to wait for a client to ask for
         *            the next batch.
         */
        public SimpleBatchingCallback(Message<JsonObject> message,
                int batchSize, int timeoutMS) {
            myMessage = message;
            myBatchSize = batchSize;
            myTimeoutMS = timeoutMS;
            myPending = new ConcurrentLinkedQueue<Document>();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to collect the documents until the batch is full to send
         * to the caller.
         * </p>
         */
        @Override
        public void callback(Document result) {
            myPending.offer(result);

            if (myBatchSize <= myPending.size()) {
                sendBatch(false);
            }
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to send the final batch.
         * </p>
         */
        @Override
        public void done() {
            sendBatch(true);
        }

        /**
         * {@inheritDoc}
         * <p>
         * Remembers the error.
         * </p>
         * 
         * @param thrown
         *            The thrown exception.
         */
        @Override
        public void exception(Throwable thrown) {
            myError = thrown;
            sendBatch(true);
        }

        /**
         * Sets the control object for the streaming cursor.
         * 
         * @param cursor
         *            The cursor to control.
         */
        public void setCursor(MongoStreamCursorControl cursor) {
            myCursorControl = cursor;
        }

        /**
         * Create the message for a batch of documents.
         * 
         * @param status
         *            The status of the batch.
         * @param results
         *            The raw results.
         * @return The JsonObject result.
         */
        private JsonObject createBatchMessage(String status, JsonArray results) {
            JsonObject reply = new JsonObject();

            reply.putArray("results", results);
            reply.putString("status", status);
            reply.putNumber("number", results.size());

            return reply;
        }

        /**
         * Sends the next batch of messages to the client.
         * 
         * @param done
         *            Set to true on a terminal event.
         */
        private void sendBatch(boolean done) {
            myCursorControl.pause();

            if (done && (myError != null)) {
                sendError(myMessage, myError);
            }

            JsonArray results = new JsonArray();
            Document doc = null;
            while ((results.size() < myBatchSize)
                    && (doc = myPending.poll()) != null) {
                JsonObject m = convert(doc);
                results.add(m);
            }

            String status = (done && (myError == null)) ? "ok" : "more-exist";
            JsonObject reply = createBatchMessage(status, results);
            if (done) {
                sendOK(myMessage, reply);
            }
            else {
                // If the user doesn't reply within timeout, close the cursor
                final long timerID = getVertx().setTimer(myTimeoutMS,
                        new Handler<Long>() {
                            @Override
                            public void handle(Long id) {
                                getContainer().logger().warn(
                                        "Closing MongoDB cursor on timeout");
                                myCursorControl.close();
                            }
                        });

                myMessage.reply(reply, new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> msg) {
                        getVertx().cancelTimer(timerID);

                        myMessage = msg;

                        myCursorControl.resume();
                    }
                });
            }
        }
    }

    /**
     * DocumentCallback provides the callback for actions returning a single
     * document.
     * 
     * @api.no This class is <b>NOT</b> part of the drivers API. This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver.
     * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected final class DocumentCallback implements Callback<Document> {

        /** The name of the reply field. */
        private final String myFieldName;

        /** The original message. */
        private final Message<JsonObject> myMessage;

        /** The reply to the request. */
        private final JsonObject myReply;

        /**
         * Creates a new DocumentCallback.
         * 
         * @param reply
         *            The reply to the request.
         * @param message
         *            The original message.
         */
        public DocumentCallback(JsonObject reply, Message<JsonObject> message) {
            myReply = reply;
            myMessage = message;
            myFieldName = "result";
        }

        /**
         * Creates a new DocumentCallback.
         * 
         * @param message
         *            The original message.
         */
        public DocumentCallback(Message<JsonObject> message) {
            this((JsonObject) null, message);
        }

        /**
         * Creates a new DocumentCallback.
         * 
         * @param fieldName
         *            The name of the field in the reply.
         * @param message
         *            The original message.
         */
        public DocumentCallback(String fieldName, Message<JsonObject> message) {
            myReply = null;
            myMessage = message;
            myFieldName = fieldName;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to reply based on the results.
         * </p>
         */
        @Override
        public void callback(Document result) {
            JsonObject reply = (myReply != null) ? myReply.copy()
                    : new JsonObject();
            if (result != null) {
                reply.putObject(myFieldName, convert(result));
            }
            // else leave the field blank so it is 'undefined'.

            sendOK(myMessage, reply);
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to reply that there was an error.
         * </p>
         */
        @Override
        public void exception(Throwable thrown) {
            sendError(myMessage, thrown);
        }
    }

    /**
     * ResultCallback provides the callback for most actions.
     * 
     * @param <T>
     *            The expected response type.
     * 
     * @api.no This class is <b>NOT</b> part of the drivers API. This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver.
     * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected final class ResultCallback<T extends Number> implements
            Callback<T> {

        /** The name of the reply value field. */
        private final String myFieldName;

        /** The original message. */
        private final Message<JsonObject> myMessage;

        /** The reply to the request. */
        private final JsonObject myReply;

        /**
         * Creates a new ResultCallback.
         * 
         * @param reply
         *            The reply to the request.
         * @param message
         *            The original message.
         */
        public ResultCallback(JsonObject reply, Message<JsonObject> message) {
            myFieldName = "number";
            myReply = reply;
            myMessage = message;
        }

        /**
         * Creates a new ResultCallback.
         * 
         * @param message
         *            The original message.
         */
        public ResultCallback(Message<JsonObject> message) {
            this((JsonObject) null, message);
        }

        /**
         * Creates a new ResultCallback.
         * 
         * @param fieldName
         *            The field name for the value.
         * @param message
         *            The original message.
         */
        public ResultCallback(String fieldName, Message<JsonObject> message) {
            myFieldName = fieldName;
            myReply = null;
            myMessage = message;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to reply based on the results.
         * </p>
         */
        @Override
        public void callback(T result) {
            JsonObject reply = (myReply != null) ? myReply.copy()
                    : new JsonObject();
            reply.putNumber(myFieldName, result);

            sendOK(myMessage, reply);
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to reply that there was an error.
         * </p>
         */
        @Override
        public void exception(Throwable thrown) {
            sendError(myMessage, thrown);
        }
    }
}
