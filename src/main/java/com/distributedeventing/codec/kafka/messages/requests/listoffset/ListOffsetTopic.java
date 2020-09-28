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
package com.distributedeventing.codec.kafka.messages.requests.listoffset;

import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.values.String;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.values.Array;
import com.distributedeventing.codec.kafka.values.CompositeValue;
import com.distributedeventing.codec.kafka.values.ValueFactory;

public final class ListOffsetTopic extends CompositeValue {

    /** Name of the topic to fetch offsets for **/
    public static final Field NAME = new Field("name");

    /** Partitions to fetch offsets for **/
    public static final Field PARTITIONS = new Field("partitions");

    public static final Schema SCHEMA_V4 = new Schema(NAME,
                                                      PARTITIONS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V4};

    private final String name = new String();

    private final ListOffsetPartition.ListOffsetPartitionFactory partitionFactory;
    {
        if (schema == SCHEMA_V4) {
            partitionFactory = new ListOffsetPartition.ListOffsetPartitionFactory((short) 4);
        } else {
            partitionFactory = null;
        }
    }

    private final Array<ListOffsetPartition> partitions = new Array<>(partitionFactory);

    public static ListOffsetTopic getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new ListOffsetTopic(SCHEMAS[version]);
    }

    public void name(final byte[] nameBytes) {
        name.value(nameBytes);
    }

    public void name(final byte[] nameBytes,
                     final int offset,
                     final int length) {
        name.value(nameBytes, offset, length);
    }

    public ListOffsetPartition createPartition() {
        final ListOffsetPartition partition = partitionFactory.createInstance();
        partitions.add(partition);
        return partition;
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(NAME, name);
        fieldValueBindings.put(PARTITIONS, partitions);
    }

    private ListOffsetTopic(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class ListOffsetTopicFactory implements ValueFactory<ListOffsetTopic> {

        private final short version;

        public ListOffsetTopicFactory(final short version) {
            this.version = version;
        }

        @Override
        public ListOffsetTopic createInstance() {
            return ListOffsetTopic.getInstance(version);
        }
    }
}