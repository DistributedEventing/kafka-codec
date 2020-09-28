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
package com.distributedeventing.codec.kafka.messages.requests.fetch;

import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.values.String;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.values.Array;
import com.distributedeventing.codec.kafka.values.CompositeValue;
import com.distributedeventing.codec.kafka.values.ValueFactory;

public final class FetchableTopic extends CompositeValue {

    /** Name of the topic to fetch **/
    public static final Field NAME = new Field("name");

    /** The partitions of the topic to fetch **/
    public static final Field FETCH_PARTITIONS = new Field("fetch_partitions");

    public static final Schema SCHEMA_V9 = new Schema(NAME,
                                                      FETCH_PARTITIONS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V9};

    private final String name = new String();

    private final FetchPartition.FetchPartitionFactory fetchPartitionFactory;
    {
        if (schema == SCHEMA_V9) {
            fetchPartitionFactory = new FetchPartition.FetchPartitionFactory((short) 9);
        } else {
            fetchPartitionFactory = null;
        }
    }

    private final Array<FetchPartition> fetchPartitions = new Array<>(fetchPartitionFactory);

    public static FetchableTopic getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new FetchableTopic(SCHEMAS[version]);
    }

    public void name(final byte[] nameBytes) {
        name.value(nameBytes);
    }

    public void name(final byte[] nameBytes,
                     final int offset,
                     final int length) {
        name.value(nameBytes, offset, length);
    }

    public FetchPartition createFetchPartition() {
        final FetchPartition fetchPartition = fetchPartitionFactory.createInstance();
        fetchPartitions.add(fetchPartition);
        return fetchPartition;
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(NAME, name);
        fieldValueBindings.put(FETCH_PARTITIONS, fetchPartitions);
    }

    private FetchableTopic(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class FetchableTopicFactory implements ValueFactory<FetchableTopic> {

        private final short version;

        public FetchableTopicFactory(final short version) {
            this.version = version;
        }

        @Override
        public FetchableTopic createInstance() {
            return FetchableTopic.getInstance(version);
        }
    }
}