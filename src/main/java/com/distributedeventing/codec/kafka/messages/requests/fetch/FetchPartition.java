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
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.values.CompositeValue;
import com.distributedeventing.codec.kafka.values.Int32;
import com.distributedeventing.codec.kafka.values.Int64;
import com.distributedeventing.codec.kafka.values.ValueFactory;

public final class FetchPartition extends CompositeValue {

    /** The index of the partition to fetch **/
    public static final Field PARTITION_INDEX = new Field("partition_index");

    /** Epoch of the current leader of the partition **/
    public static final Field CURRENT_LEADER_EPOCH = new Field("current_leader_epoch");

    /** The offset to start fetching at **/
    public static final Field FETCH_OFFSET = new Field("fetch_offset");

    /**
     * This field only applies to follower replicas. In those cases, it
     * should be set to the earliest available offset of the follower
     * replica. There is no need to set this field for consumers.
     */
    public static final Field LOG_START_OFFSET = new Field("log_start_offset");

    /**
     * Maximum number of bytes to fetch from the partition. This limit may
     * not be honored under certain circumstances, for example when the
     * size of the first message to send exceeds the limit.
     */
    public static final Field MAX_BYTES = new Field("max_bytes");

    public static final Schema SCHEMA_V9 = new Schema(PARTITION_INDEX,
                                                      CURRENT_LEADER_EPOCH,
                                                      FETCH_OFFSET,
                                                      LOG_START_OFFSET,
                                                      MAX_BYTES);

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

    private final Int32 partitionIndex = new Int32();

    private final Int32 currentLeaderEpoch = new Int32();

    private final Int64 fetchOffset = new Int64();

    private final Int64 logStartOffset = new Int64();

    private final Int32 maxBytes = new Int32();

    public static FetchPartition getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new FetchPartition(SCHEMAS[version]);
    }

    public void partitionIndex(final int idx) {
        partitionIndex.value(idx);
    }

    public void currentLeaderEpoch(final int epoch) {
        currentLeaderEpoch.value(epoch);
    }

    public void fetchOffset(final long offset) {
        fetchOffset.value(offset);
    }

    public void logStartOffset(final long offset) {
        logStartOffset.value(offset);
    }

    public void maxBytes(final int bytes) {
        maxBytes.value(bytes);
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(PARTITION_INDEX, partitionIndex);
        fieldValueBindings.put(CURRENT_LEADER_EPOCH, currentLeaderEpoch);
        fieldValueBindings.put(FETCH_OFFSET, fetchOffset);
        fieldValueBindings.put(LOG_START_OFFSET, logStartOffset);
        fieldValueBindings.put(MAX_BYTES, maxBytes);
    }

    private FetchPartition(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class FetchPartitionFactory implements ValueFactory<FetchPartition> {

        private final short version;

        public FetchPartitionFactory(final short version) {
            this.version = version;
        }

        @Override
        public FetchPartition createInstance() {
            return FetchPartition.getInstance(version);
        }
    }
}