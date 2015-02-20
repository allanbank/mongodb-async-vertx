/*
 * #%L
 * Converter.java - mod-mongo-async-persistor - Allanbank Consulting, Inc.
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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.JavaScriptElement;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.MaxKeyElement;
import com.allanbank.mongodb.bson.element.MinKeyElement;
import com.allanbank.mongodb.bson.element.MongoTimestampElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.RegularExpressionElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.SymbolElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.util.IOUtils;

/**
 * Contains utility methods to convert from {@link JsonObject JsonObjects} to
 * and from {@link Document Documents}.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be/
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class Converter {

    /** The fields for strict encoded {@link BinaryElement}. */
    public static final Set<String> BINARY_FIELDS = Collections
            .unmodifiableSet(new HashSet<>(Arrays.asList("$binary", "$type")));

    /** The fields for strict encoded {@link TimestampElement}. */
    public static final Set<String> DATE_FIELDS = Collections
            .unmodifiableSet(Collections.singleton("$date"));

    /** The format for the date strings. */
    public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    /** The fields for strict encoded {@link LongElement}. */
    public static final Set<String> LONG_FIELDS = Collections
            .unmodifiableSet(Collections.singleton("$numberLong"));

    /** The fields for strict encoded {@link MaxKeyElement}. */
    public static final Set<String> MAX_KEY_FIELDS = Collections
            .unmodifiableSet(Collections.singleton("$maxKey"));

    /** The fields for strict encoded {@link MinKeyElement}. */
    public static final Set<String> MIN_KEY_FIELDS = Collections
            .unmodifiableSet(Collections.singleton("$minKey"));

    /** The fields for strict encoded {@link ObjectIdElement}. */
    public static final Set<String> OID_FIELDS = Collections
            .unmodifiableSet(Collections.singleton("$oid"));

    /** The fields for strict encoded {@link RegularExpressionElement}. */
    public static final Set<String> REGEX_FIELDS = Collections
            .unmodifiableSet(new HashSet<>(Arrays.asList("$regex", "$options")));

    /** The fields for strict encoded {@link MongoTimestampElement}. */
    public static final Set<String> TIMESTAMP_FIELDS = Collections
            .unmodifiableSet(Collections.singleton("$timestamp"));

    /** The sub-fields for strict encoded {@link MongoTimestampElement}. */
    public static final Set<String> TIMESTAMP_SUB_FIELDS = Collections
            .unmodifiableSet(new HashSet<>(Arrays.asList("t", "i")));

    /** The default time zone. */
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    /**
     * Converts the {@link ArrayElement} into a {@link JsonArray}.
     *
     * @param arrayElement
     *            The array element to be converted.
     * @return The converted array.
     */
    @SuppressWarnings("deprecation")
    public static JsonArray convert(final ArrayElement arrayElement) {
        final JsonArray array = new JsonArray();
        for (final Element e : arrayElement.getEntries()) {
            switch (e.getType()) {
            case ARRAY: {
                array.addArray(convert((ArrayElement) e));
                break;
            }
            case BINARY: {
                final BinaryElement binary = (BinaryElement) e;

                array.addObject(new JsonObject().putString("$binary",
                        IOUtils.toBase64(binary.getValue())).putString("$type",
                                Integer.toHexString(binary.getSubType())));
                break;
            }
            case BOOLEAN: {
                array.addBoolean(((BooleanElement) e).getValue());
                break;
            }
            case DB_POINTER: {
                throw new IllegalArgumentException(
                        "Cannot convert a DB Pointer to a JSON element.");
            }
            case DOCUMENT: {
                array.addObject(convert(((DocumentElement) e).getDocument()));
                break;
            }
            case DOUBLE: {
                array.addNumber(((DoubleElement) e).getValue());
                break;
            }
            case INTEGER: {
                array.addNumber(((IntegerElement) e).getValue());
                break;
            }
            case JAVA_SCRIPT: {
                array.addObject(new JsonObject(((JavaScriptElement) e)
                        .getJavaScript()));
                break;
            }
            case JAVA_SCRIPT_WITH_SCOPE: {
                throw new IllegalArgumentException(
                        "Cannot convert a JavaScript with Scope to a JSON element.");
            }
            case LONG: {
                array.addNumber(((LongElement) e).getValue());
                break;
            }
            case MAX_KEY: {
                array.addObject(new JsonObject().putNumber("$maxKey", 1));
                break;
            }
            case MIN_KEY: {
                array.addObject(new JsonObject().putNumber("$minKey", 1));
                break;
            }
            case MONGO_TIMESTAMP: {
                final MongoTimestampElement ts = (MongoTimestampElement) e;

                final long value = ts.getTime();
                final long time = (value >> Integer.SIZE) & 0xFFFFFFFFL;
                final long increment = value & 0xFFFFFFFFL;

                array.addObject(new JsonObject().putObject(
                        "$timestamp",
                        new JsonObject().putNumber("t", time).putNumber("i",
                                increment)));
                break;
            }
            case NULL: {
                array.add(null);
                break;
            }
            case OBJECT_ID: {
                final ObjectIdElement oid = (ObjectIdElement) e;

                array.addObject(new JsonObject().putString("$oid", oid
                        .getValueAsObject().toHexString()));
                break;
            }
            case REGEX: {
                final RegularExpressionElement regex = (RegularExpressionElement) e;

                final String pattern = regex.getPattern();
                final String options = regex.getOptionsAsString();

                array.addObject(new JsonObject().putString("$regex", pattern)
                        .putString("$options", options));
                break;
            }
            case STRING: {
                array.addString(((StringElement) e).getValue());
                break;
            }
            case SYMBOL: {
                array.addString(((SymbolElement) e).getSymbol());
                break;
            }
            case UTC_TIMESTAMP: {
                final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
                sdf.setTimeZone(UTC);

                final TimestampElement ts = (TimestampElement) e;

                final String date = sdf.format(ts.getValueAsObject());

                array.addObject(new JsonObject().putString("$date", date));
                break;
            }
            default:
                break;
            }
        }
        return array;
    }

    /**
     * Converts the {@link Document} into a {@link JsonObject}.
     *
     * @param doc
     *            The {@link Document} to convert.
     * @return The {@link JsonObject} constructed based on the {@link Document}.
     */
    @SuppressWarnings("deprecation")
    public static JsonObject convert(final DocumentAssignable doc) {

        final JsonObject obj = new JsonObject();
        for (final Element e : doc.asDocument()) {
            switch (e.getType()) {
            case ARRAY: {
                obj.putArray(e.getName(), convert((ArrayElement) e));
                break;
            }
            case BINARY: {
                final BinaryElement binary = (BinaryElement) e;

                obj.putObject(
                        e.getName(),
                        new JsonObject().putString("$binary",
                                IOUtils.toBase64(binary.getValue())).putString(
                                        "$type",
                                        Integer.toHexString(binary.getSubType())));
                break;
            }
            case BOOLEAN: {
                obj.putBoolean(e.getName(), ((BooleanElement) e).getValue());
                break;
            }
            case DB_POINTER: {
                throw new IllegalArgumentException(
                        "Cannot convert a DB Pointer to a JSON element.");
            }
            case DOCUMENT: {
                obj.putObject(e.getName(),
                        convert(((DocumentElement) e).getDocument()));
                break;
            }
            case DOUBLE: {
                obj.putNumber(e.getName(), ((DoubleElement) e).getValue());
                break;
            }
            case INTEGER: {
                obj.putNumber(e.getName(), ((IntegerElement) e).getValue());
                break;
            }
            case JAVA_SCRIPT: {
                obj.putObject(e.getName(), new JsonObject(
                        ((JavaScriptElement) e).getJavaScript()));
                break;
            }
            case JAVA_SCRIPT_WITH_SCOPE: {
                throw new IllegalArgumentException(
                        "Cannot convert a JavaScript with Scope to a JSON element.");
            }
            case LONG: {
                obj.putNumber(e.getName(), ((LongElement) e).getValue());
                break;
            }
            case MAX_KEY: {
                obj.putObject(e.getName(),
                        new JsonObject().putNumber("$maxKey", 1));
                break;
            }
            case MIN_KEY: {
                obj.putObject(e.getName(),
                        new JsonObject().putNumber("$minKey", 1));
                break;
            }
            case MONGO_TIMESTAMP: {
                final MongoTimestampElement ts = (MongoTimestampElement) e;

                final long value = ts.getTime();
                final long time = (value >> Integer.SIZE) & 0xFFFFFFFFL;
                final long increment = value & 0xFFFFFFFFL;

                obj.putObject(e.getName(), new JsonObject().putObject(
                        "$timestamp", new JsonObject().putNumber("t", time)
                        .putNumber("i", increment)));
                break;
            }
            case NULL: {
                obj.putObject(e.getName(), null);
                break;
            }
            case OBJECT_ID: {
                final ObjectIdElement oid = (ObjectIdElement) e;

                obj.putObject(e.getName(), new JsonObject().putString("$oid",
                        oid.getValueAsObject().toHexString()));
                break;
            }
            case REGEX: {
                final RegularExpressionElement regex = (RegularExpressionElement) e;

                final String pattern = regex.getPattern();
                final String options = regex.getOptionsAsString();

                obj.putObject(e.getName(),
                        new JsonObject().putString("$regex", pattern)
                        .putString("$options", options));
                break;
            }
            case STRING: {
                obj.putString(e.getName(), ((StringElement) e).getValue());
                break;
            }
            case SYMBOL: {
                obj.putString(e.getName(), ((SymbolElement) e).getSymbol());
                break;
            }
            case UTC_TIMESTAMP: {
                final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
                sdf.setTimeZone(UTC);

                final TimestampElement ts = (TimestampElement) e;

                final String date = sdf.format(ts.getValueAsObject());

                obj.putObject(e.getName(),
                        new JsonObject().putString("$date", date));
                break;
            }
            default:
                break;
            }
        }
        return obj;
    }

    /**
     * Converts the JsonObject to a Document.
     *
     * @param object
     *            The {@link JsonObject}.
     * @return The converted document.
     */
    public static Document convert(final JsonObject object) {
        final DocumentBuilder doc = BuilderFactory.start();

        for (final String fieldName : object.getFieldNames()) {
            final Object value = object.getValue(fieldName);

            if (value instanceof JsonArray) {
                convert(doc.pushArray(fieldName), (JsonArray) value);
            }
            else if (value instanceof JsonObject) {
                final JsonObject json = (JsonObject) value;

                doc.add(toElement(fieldName, json));
            }
            else {
                doc.add(fieldName, value);
            }
        }

        return doc.build();
    }

    /**
     * Converts the {@link JsonArray} and append the results to the
     * {@link ArrayBuilder}.
     *
     * @param arrayBuilder
     *            The builder to populate.
     * @param array
     *            The raw {@link JsonArray}.
     */
    protected static void convert(final ArrayBuilder arrayBuilder,
            final JsonArray array) {
        final int size = array.size();
        for (int i = 0; i < size; ++i) {
            final Object value = array.get(i);

            if (value instanceof JsonArray) {
                convert(arrayBuilder.pushArray(), (JsonArray) value);
            }
            else if (value instanceof JsonObject) {
                arrayBuilder.add(toElement("0", (JsonObject) value));
            }
            else {
                arrayBuilder.add(value);
            }
        }
    }

    /**
     * Converts the {@link JsonObject} to an {@link Element} by interpreting the
     * document structures.
     *
     * @param fieldName
     *            The name of the element.
     * @param json
     *            The JSON document to interpret.
     * @return The Element.
     */
    private static final Element toElement(final String fieldName,
            final JsonObject json) {
        final Set<String> fieldNames = json.getFieldNames();
        if (BINARY_FIELDS.equals(fieldNames)) {
            // "$binary", "$type"
            return new BinaryElement(fieldName, (byte) Integer.parseInt(
                    json.getString("$type"), 16), IOUtils.base64ToBytes(json
                            .getString("$binary")));
        }
        else if (DATE_FIELDS.equals(fieldNames)) {
            // $date
            final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
            sdf.setTimeZone(UTC);

            try {
                return new TimestampElement(fieldName, sdf.parse(
                        json.getString("$date")).getTime());
            }
            catch (final IllegalArgumentException e) {
                // Fall-through.
            }
            catch (final ParseException e) {
                // Fall-through.
            }
        }
        else if (TIMESTAMP_FIELDS.equals(fieldNames)) {
            // $timestamp : { "t" : <int>, "i" <int> }
            final JsonObject subObject = json.getObject("$timestamp");
            if (TIMESTAMP_SUB_FIELDS.equals(subObject.getFieldNames())) {
                final long time = subObject.getLong("t");
                final long incr = subObject.getLong("i");

                return new MongoTimestampElement(fieldName,
                        (time << Integer.SIZE) | incr);
            }
        }
        else if (REGEX_FIELDS.equals(fieldNames)) {
            // $regex $options
            return new RegularExpressionElement(fieldName,
                    json.getString("$regex"), json.getString("$options"));
        }
        else if (OID_FIELDS.equals(fieldNames)) {
            // $oid
            return new ObjectIdElement(fieldName, new ObjectId(
                    json.getString("$oid")));
        }
        else if (MIN_KEY_FIELDS.equals(fieldNames)) {
            // $minKey
            return new MinKeyElement(fieldName);
        }
        else if (MAX_KEY_FIELDS.equals(fieldNames)) {
            // $maxKey
            return new MaxKeyElement(fieldName);
        }
        else if (LONG_FIELDS.equals(fieldNames)) {
            // $numberLong
            return new LongElement(fieldName, Long.parseLong(json
                    .getString("$numberLong")));
        }

        return new DocumentElement(fieldName, convert(json));
    }

    /**
     * Creates a new Converter.
     */
    private Converter() {
        // Nothing.
    }
}
