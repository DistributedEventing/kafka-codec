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

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.Array;
import com.distributedeventing.codec.kafka.values.CompositeValue;
import com.distributedeventing.codec.kafka.values.String;
import com.distributedeventing.codec.kafka.values.ValueFactory;

public final class FetchableTopicResponse extends CompositeValue {

    /** Name of the topic **/
    public static final Field NAME = new Field("name");

    /** Partitions of the topic **/
    public static final Field PARTITIONS = new Field("partitions");

    public static final Schema SCHEMA_V11 = new Schema(NAME,
                                                       PARTITIONS);

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

    private final String name = new String();

    private final Array<FetchablePartitionResponse> partitions;
    {
        if (schema == SCHEMA_V11) {
            partitions = new Array<>(new FetchablePartitionResponse.FetchablePartitionResponseFactory((short) 11));
        } else {
            partitions = null;
        }
    }

    public static FetchableTopicResponse getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new FetchableTopicResponse(SCHEMAS[version]);
    }

    public String topic() {
        return name;
    }

    public Array<FetchablePartitionResponse>.ElementIterator partitions() {
        return partitions.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(NAME, name);
        fieldValueBindings.put(PARTITIONS, partitions);
    }

    private FetchableTopicResponse(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class FetchableTopicResponseFactory implements ValueFactory<FetchableTopicResponse> {

        private final short version;

        public FetchableTopicResponseFactory(final short version) {
            this.version = version;
        }

        @Override
        public FetchableTopicResponse createInstance() {
            return FetchableTopicResponse.getInstance(version);
        }
    }
}