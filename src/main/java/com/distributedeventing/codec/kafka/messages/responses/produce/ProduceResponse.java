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
package com.distributedeventing.codec.kafka.messages.responses.produce;

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.headers.ResponseHeader;
import com.distributedeventing.codec.kafka.messages.responses.ResponseMessage;
import com.distributedeventing.codec.kafka.values.Array;
import com.distributedeventing.codec.kafka.values.Int32;

public final class ProduceResponse extends ResponseMessage {

    /** The produce responses **/
    public static final Field RESPONSES = new Field("responses");

    /**
     * The amount of time for which the produce request was throttled due
     * to a quota violation. This will be 0 if there was no violation.
     */
    public static final Field THROTTLE_TIME_MS = new Field("throttle_time_ms");

    public static final Schema SCHEMA_V8 = new Schema(RESPONSES,
                                                      THROTTLE_TIME_MS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V8};

    private final Array<TopicProduceResponse> responses;
    {
        if (schema == SCHEMA_V8) {
            responses = new Array<>(new TopicProduceResponse.TopicProduceResponseFactory((short) 8));
        } else {
            responses = null;
        }
    }

    private final Int32 throttleTimeMs = new Int32();

    public static ProduceResponse getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new ProduceResponse(createResponseHeader(version),
                                   SCHEMAS[version]);
    }

    private static ResponseHeader createResponseHeader(final short version) {
        final short headerVersion = (short) 0;
        final ResponseHeader header = ResponseHeader.getInstance(headerVersion);
        return header;
    }

    public Array<TopicProduceResponse>.ElementIterator responses() {
        return responses.iterator();
    }

    public Int32 throttleTimeMs() {
        return throttleTimeMs;
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(RESPONSES, responses);
        fieldValueBindings.put(THROTTLE_TIME_MS, throttleTimeMs);
    }

    private ProduceResponse(final ResponseHeader header,
                            final Schema schema) {
        super(header, schema);
        setFieldValueBindings();
    }
}