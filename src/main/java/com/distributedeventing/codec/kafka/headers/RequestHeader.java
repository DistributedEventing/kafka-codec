/*
 * Copyright 2020 The DistributedEventing Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.distributedeventing.codec.kafka.headers;

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.values.*;
import io.netty.buffer.ByteBuf;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;

public final class RequestHeader extends AbstractHeader {

    /** API key of the request **/
    public static final Field REQUEST_API_KEY = new Field("request_api_key");

    /** API version of the request **/
    public static final Field REQUEST_API_VERSION = new Field("request_api_version");

    /**
     * Correlation ID  of the request. A response for a request will have the same
     * correlation ID. This is how Kafka maps requests to responses.
     */
    public static final Field CORRELATION_ID = new Field("correlation_id");

    /** Identifier of the Kafka client **/
    public static final Field CLIENT_ID = new Field("client_id");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V0 = new Schema(REQUEST_API_KEY,
                                                      REQUEST_API_VERSION,
                                                      CORRELATION_ID);

    public static final Schema SCHEMA_V1 = new Schema(REQUEST_API_KEY,
                                                      REQUEST_API_VERSION,
                                                      CORRELATION_ID,
                                                      CLIENT_ID);

    public static final Schema SCHEMA_V2 = new Schema(REQUEST_API_KEY,
                                                      REQUEST_API_VERSION,
                                                      CORRELATION_ID,
                                                      CLIENT_ID,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {SCHEMA_V0,
                                                         SCHEMA_V1,
                                                         SCHEMA_V2};

    private final Int16 requestApiKey = new Int16();

    private final Int16 requestApiVersion = new Int16();

    private final Int32 correlationId = new Int32();

    private final NullableString clientId = new NullableString();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static RequestHeader getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] ==  null) {
            throw new UnsupportedVersionException(version);
        }
        return new RequestHeader(version, SCHEMAS[version]);
    }

    public void writeTo(final ByteBuf buffer) {
        fieldsIterator.reset();
        while (fieldsIterator.hasNext()) {
            final Field field = fieldsIterator.next();
            final Value value = fieldValueBindings.get(field);
            value.writeTo(buffer);
        }
    }

    public void requestApiKey(final short key) {
        requestApiKey.value(key);
    }

    public void requestApiVersion(final short version) {
        requestApiVersion.value(version);
    }

    public void correlationId(final int id) {
        correlationId.value(id);
    }

    public void clientId(final byte[] clientIdBytes) {
        clientId(clientIdBytes, 0, clientIdBytes.length);
    }

    public void clientId(final byte[] clientIdBytes,
                         final int offset,
                         final int length) {
        clientId.value(clientIdBytes, offset, length);
    }

    public void taggedFields(final TaggedBytes...taggedBytes) {
        taggedFields.value(taggedBytes);
    }

    @Override
    protected void setFieldValueBindings() {
        fieldValueBindings.put(REQUEST_API_KEY, requestApiKey);
        fieldValueBindings.put(REQUEST_API_VERSION, requestApiVersion);
        fieldValueBindings.put(CORRELATION_ID, correlationId);
        fieldValueBindings.put(CLIENT_ID, clientId);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private RequestHeader(final short version,
                          final Schema schema) {
        super(version,
              schema);
        setFieldValueBindings();
    }
}