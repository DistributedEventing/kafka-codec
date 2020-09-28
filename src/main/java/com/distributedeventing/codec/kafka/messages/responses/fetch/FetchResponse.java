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
package com.distributedeventing.codec.kafka.messages.responses.fetch;

import com.distributedeventing.codec.kafka.BrokerError;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.headers.ResponseHeader;
import com.distributedeventing.codec.kafka.messages.responses.ResponseMessage;
import com.distributedeventing.codec.kafka.values.Array;
import com.distributedeventing.codec.kafka.values.Int16;
import com.distributedeventing.codec.kafka.values.Int32;

public final class FetchResponse extends ResponseMessage {

    /**
     * The amount of time for which the fetch request was throttled due
     * to a quota violation. This will be 0 if there was no violation.
     */
    public static final Field THROTTLE_TIME_MS = new Field("throttle_time_ms");

    /**
     * The top level response error code. Will be
     * {@link BrokerError#NONE} if no error
     * occurred.
     */
    public static final Field ERROR_CODE = new Field("error_code");

    /**
     * The fetch session ID or 0 if the consumer if not part of any
     * fetch session.
     */
    public static final Field SESSION_ID = new Field("session_id");

    /** The response topics **/
    public static final Field TOPICS = new Field("topics");

    public static final Schema SCHEMA_V11 = new Schema(THROTTLE_TIME_MS,
                                                       ERROR_CODE,
                                                       SESSION_ID,
                                                       TOPICS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V11};

    private final Int32 throttleTimeMs = new Int32();

    private final Int16 errorCode = new Int16();

    private final Int32 sessionId = new Int32();

    private final Array<FetchableTopicResponse> topics;
    {
        if (schema == SCHEMA_V11) {
            topics = new Array<>(new FetchableTopicResponse.FetchableTopicResponseFactory((short) 11));
        } else {
            topics = null;
        }
    }

    public static FetchResponse getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new FetchResponse(createResponseHeader(version),
                                 SCHEMAS[version]);
    }

    private static ResponseHeader createResponseHeader(final short version) {
        final short headerVersion = (short) 0;
        final ResponseHeader header = ResponseHeader.getInstance(headerVersion);
        return header;
    }

    public Int32 throttleTimeMs() {
        return throttleTimeMs;
    }

    public Int16 errorCode() {
        return errorCode;
    }

    public Int32 sessionId() {
        return sessionId;
    }

    public Array<FetchableTopicResponse>.ElementIterator topics() {
        return topics.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(THROTTLE_TIME_MS, throttleTimeMs);
        fieldValueBindings.put(ERROR_CODE, errorCode);
        fieldValueBindings.put(SESSION_ID, sessionId);
        fieldValueBindings.put(TOPICS, topics);
    }

    private FetchResponse(final ResponseHeader header,
                          final Schema schema) {
        super(header,
              schema);
        setFieldValueBindings();
    }
}