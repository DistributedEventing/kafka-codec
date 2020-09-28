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

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.CompositeValue;
import com.distributedeventing.codec.kafka.values.Int32;
import com.distributedeventing.codec.kafka.values.Int64;
import com.distributedeventing.codec.kafka.values.ValueFactory;

public final class ListOffsetPartition extends CompositeValue {

    /** Index of the partition to fetch offset for **/
    public static final Field PARTITION_INDEX = new Field("partition_index");

    /** Epoch of the current leader of topic partition **/
    public static final Field CURRENT_LEADER_EPOCH = new Field("current_leader_epoch");

    /** The current timestamp **/
    public static final Field TIMESTAMP = new Field("timestamp");

    public static final Schema SCHEMA_V4 = new Schema(PARTITION_INDEX,
                                                      CURRENT_LEADER_EPOCH,
                                                      TIMESTAMP);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V4};

    private Int32 partitionIndex = new Int32();

    private Int32 currentLeaderEpoch = new Int32();

    private Int64 timestamp = new Int64();

    public static ListOffsetPartition getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new ListOffsetPartition(SCHEMAS[version]);
    }

    public void partitionIndex(final int id) {
        partitionIndex.value(id);
    }

    public void currentLeaderEpoch(final int epoch) {
        currentLeaderEpoch.value(epoch);
    }

    public void timestamp(final long ts) {
        timestamp.value(ts);
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(PARTITION_INDEX, partitionIndex);
        fieldValueBindings.put(CURRENT_LEADER_EPOCH, currentLeaderEpoch);
        fieldValueBindings.put(TIMESTAMP, timestamp);
    }

    private ListOffsetPartition(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class ListOffsetPartitionFactory implements ValueFactory<ListOffsetPartition> {

        private final short version;

        public ListOffsetPartitionFactory(final short version) {
            this.version = version;
        }

        @Override
        public ListOffsetPartition createInstance() {
            return ListOffsetPartition.getInstance(version);
        }
    }
}