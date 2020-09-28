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
package com.distributedeventing.codec.kafka.messages.responses.apiversions;

import com.distributedeventing.codec.kafka.BrokerError;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.headers.ResponseHeader;
import com.distributedeventing.codec.kafka.messages.responses.ResponseMessage;
import com.distributedeventing.codec.kafka.values.*;

public final class ApiVersionsResponse extends ResponseMessage {

    /**
     * Response error code. Will be {@link BrokerError#NONE}
     * if no error occurred
     */
    public static final Field ERROR_CODE = new Field("error_code");

    /** Apis supported by the broker **/
    public static final Field API_KEYS = new Field("api_keys");

    /**
     * The amount of time for which the api versions request was throttled due
     * to a quota violation. This will be 0 if there was no violation.
     */
    public static final Field THROTTLE_TIME_MS = new Field("throttle_time_ms");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V3 = new Schema(ERROR_CODE,
                                                      API_KEYS,
                                                      THROTTLE_TIME_MS,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         SCHEMA_V3};

    private final Int16 errorCode = new Int16();

    private final CompactArray<ApiVersionsResponseKey> apiKeys;
    {
        if (schema == SCHEMA_V3) {
            apiKeys = new CompactArray<>(new ApiVersionsResponseKey.ApiVersionsResponseKeyFactory((short) 3));
        } else {
            apiKeys = null;
        }
    }

    private final Int32 throttleTimeMs = new Int32();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static ApiVersionsResponse getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        final ResponseHeader header = ResponseHeader.getInstance((short) 0);
        return new ApiVersionsResponse(header,
                                       SCHEMAS[version]);
    }

    public Int16 errorCode() {
        return errorCode;
    }

    public Array<ApiVersionsResponseKey>.ElementIterator apiKeys() {
        return apiKeys.iterator();
    }

    public Int32 throttleTimeMs() {
        return throttleTimeMs;
    }

    public Array<TaggedBytes>.ElementIterator taggedFields() {
        return taggedFields.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(ERROR_CODE, errorCode);
        fieldValueBindings.put(API_KEYS, apiKeys);
        fieldValueBindings.put(THROTTLE_TIME_MS, throttleTimeMs);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private ApiVersionsResponse(final ResponseHeader header,
                                final Schema schema) {
        super(header,
              schema);
        setFieldValueBindings();
    }
}