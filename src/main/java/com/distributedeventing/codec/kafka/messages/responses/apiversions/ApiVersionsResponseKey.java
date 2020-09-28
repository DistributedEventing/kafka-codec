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

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.*;
import com.distributedeventing.codec.kafka.ApiKey;

public final class ApiVersionsResponseKey extends CompositeValue {

    /** The {@link ApiKey} supported by the broker **/
    public static final Field API_KEY = new Field("api_key");

    /** Minimum version of API supported by the broker **/
    public static final Field MIN_VERSION = new Field("min_version");

    /** Maximum version of API supported by the broker **/
    public static final Field MAX_VERSION = new Field("max_version");

    public static final Field TAGGED_FIELDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V3 = new Schema(API_KEY,
                                                      MIN_VERSION,
                                                      MAX_VERSION,
                                                      TAGGED_FIELDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         SCHEMA_V3};

    private final Int16 apiKey = new Int16();

    private final Int16 minVersion = new Int16();

    private final Int16 maxVersion = new Int16();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static ApiVersionsResponseKey getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new ApiVersionsResponseKey(SCHEMAS[version]);
    }

    public Int16 apiKey() {
        return apiKey;
    }

    public Int16 minVersion() {
        return minVersion;
    }

    public Int16 maxVersion() {
        return maxVersion;
    }

    public Array<TaggedBytes>.ElementIterator taggedFields() {
        return taggedFields.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(API_KEY, apiKey);
        fieldValueBindings.put(MIN_VERSION, minVersion);
        fieldValueBindings.put(MAX_VERSION, maxVersion);
        fieldValueBindings.put(TAGGED_FIELDS, taggedFields);
    }

    private ApiVersionsResponseKey(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class ApiVersionsResponseKeyFactory implements ValueFactory<ApiVersionsResponseKey> {

        private final short version;

        public ApiVersionsResponseKeyFactory(final short version) {
            this.version = version;
        }

        @Override
        public ApiVersionsResponseKey createInstance() {
            return ApiVersionsResponseKey.getInstance(version);
        }
    }
}