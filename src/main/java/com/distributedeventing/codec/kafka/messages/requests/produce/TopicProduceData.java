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
package com.distributedeventing.codec.kafka.messages.requests.produce;

import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.String;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.values.Array;
import com.distributedeventing.codec.kafka.values.CompositeValue;
import com.distributedeventing.codec.kafka.values.ValueFactory;

public final class TopicProduceData extends CompositeValue {

    /** Name of the topic to produce to **/
    public static final Field NAME = new Field("name");

    /** The partitions to produce to **/
    public static final Field PARTITIONS = new Field("partitions");

    public static final Schema SCHEMA_V0 = new Schema(NAME,
                                                      PARTITIONS);

    public static final Schema[] SCHEMAS = new Schema[] {SCHEMA_V0};

    private final String name = new String();

    private final PartitionProduceData.PartitionProduceDataFactory partitionFactory;
    {
        if (schema == SCHEMA_V0) {
            partitionFactory = new PartitionProduceData.PartitionProduceDataFactory((short) 0);
        } else  {
            partitionFactory = null;
        }
    }

    private final Array<PartitionProduceData> partitions = new Array<>(partitionFactory);

    public void name(final byte[] nameBytes) {
        name.value(nameBytes);
    }

    public void name(final byte[] nameBytes,
                     final int offset,
                     final int length) {
        name.value(nameBytes, offset, length);
    }

    public PartitionProduceData createPartition() {
        final PartitionProduceData partition = partitionFactory.createInstance();
        partitions.add(partition);
        return partition;
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(NAME, name);
        fieldValueBindings.put(PARTITIONS, partitions);
    }

    private TopicProduceData(final short version) {
        super(SCHEMAS[version]);
        setFieldValueBindings();
    }

    public static class TopicProduceDataFactory implements ValueFactory<TopicProduceData> {

        private final short version;

        public TopicProduceDataFactory(final short version) {
            this.version = version;
        }

        @Override
        public TopicProduceData createInstance() {
            return new TopicProduceData(version);
        }
    }
}