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
package com.distributedeventing.codec.kafka.messages.responses.deletetopics;

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.headers.ResponseHeader;
import com.distributedeventing.codec.kafka.messages.responses.ResponseMessage;
import com.distributedeventing.codec.kafka.values.*;

public final class DeleteTopicsResponse extends ResponseMessage {

    /**
     * The amount of time for which the delete topics request was throttled
     * due to a quota violation. This will be 0 if there was no violation.
     */
    public static final Field THROTTLE_TIME_MS = new Field("throttle_time_ms");

    /** Array of per topic deletion result **/
    public static final Field RESPONSES = new Field("responses");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V4 = new Schema(THROTTLE_TIME_MS,
                                                      RESPONSES,
                                                      TAGGED_FEILDS);

    private final Int32 throttleTimeMs = new Int32();

    private final CompactArray<DeletableTopicResult> responses;
    {
        if (schema == SCHEMA_V4) {
            responses = new CompactArray<>(new DeletableTopicResult.DeletableTopicResultFactory((short) 4));
        } else {
            responses = null;
        }
    }

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V4};

    public static DeleteTopicsResponse getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new DeleteTopicsResponse(createResponseHeader(version),
                                        SCHEMAS[version]);
    }

    private static ResponseHeader createResponseHeader(final short version) {
        final short headerVersion = (short) (version >= 4? 1: 0);
        return ResponseHeader.getInstance(headerVersion);
    }

    public Int32 throttleTimeMs() {
        return throttleTimeMs;
    }

    public Array<DeletableTopicResult>.ElementIterator responses() {
        return responses.iterator();
    }

    public Array<TaggedBytes>.ElementIterator taggedFields() {
        return taggedFields.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(THROTTLE_TIME_MS, throttleTimeMs);
        fieldValueBindings.put(RESPONSES, responses);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private DeleteTopicsResponse(final ResponseHeader header,
                                 final Schema schema) {
        super(header,
              schema);
        setFieldValueBindings();
    }
}