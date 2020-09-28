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
package com.distributedeventing.codec.kafka.messages.responses.createtopics;

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.headers.ResponseHeader;
import com.distributedeventing.codec.kafka.messages.responses.ResponseMessage;
import com.distributedeventing.codec.kafka.values.*;

public final class CreateTopicsResponse extends ResponseMessage {

    /**
     * The amount of time for which the create topics request was throttled
     * due to a quota violation. This will be 0 if there was no violation.
     */
    public static final Field THROTTLE_TIME_MS = new Field("throttle_time_ms");

    /** An array of topic results **/
    public static final Field TOPICS = new Field("topics");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V5 = new Schema(THROTTLE_TIME_MS,
                                                      TOPICS,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V5};

    private final Int32 throttleTimeMs = new Int32();

    private final CompactArray<CreatableTopicResult> topics;
    {
        if (schema == SCHEMA_V5) {
            topics = new CompactArray<>(new CreatableTopicResult.CreatableTopicResultFactory((short) 5));
        } else {
            topics = null;
        }
    }

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static CreateTopicsResponse getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new CreateTopicsResponse(createResponseHeader(version),
                                        SCHEMAS[version]);
    }

    private static ResponseHeader createResponseHeader(final short version) {
        final short headerVersion = (short) (version >= 5? 1: 0);
        return ResponseHeader.getInstance(headerVersion);
    }

    public Int32 throttleTimeMs() {
        return throttleTimeMs;
    }

    public Array<CreatableTopicResult>.ElementIterator topics() {
        return topics.iterator();
    }

    public Array<TaggedBytes>.ElementIterator taggedFields() {
        return taggedFields.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(THROTTLE_TIME_MS, throttleTimeMs);
        fieldValueBindings.put(TOPICS, topics);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private CreateTopicsResponse(final ResponseHeader header,
                                 final Schema schema) {
        super(header,
              schema);
        setFieldValueBindings();
    }
}