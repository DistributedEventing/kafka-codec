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
package com.distributedeventing.codec.kafka.messages.responses.listoffset;

import com.distributedeventing.codec.kafka.BrokerError;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.*;

public final class ListOffsetPartitionResponse extends CompositeValue {

    /** Index of the partition **/
    public static final Field PARTITION_INDEX = new Field("partition_index");

    /**
     * The error code associated with the topic partition. Will be
     * {@link BrokerError#NONE} if no error
     * occurred.
     */
    public static final Field ERROR_CODE = new Field("error_code");

    /** Timestamp associated with the offset in response **/
    public static final Field TIMESTAMP = new Field("timestamp");

    /** The returned offset **/
    public static final Field OFFSET = new Field("offset");

    /** Epoch of the leader of the topic partition **/
    public static final Field LEADER_EPOCH = new Field("leader_epoch");

    public static final Schema SCHEMA_V4 = new Schema(PARTITION_INDEX,
                                                      ERROR_CODE,
                                                      TIMESTAMP,
                                                      OFFSET,
                                                      LEADER_EPOCH);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V4};

    private final Int32 partitionId = new Int32();

    private final Int16 errorCode = new Int16();

    private final Int64 timestamp = new Int64();

    private final Int64 offset = new Int64();

    private final Int32 leaderEpoch = new Int32();

    public static ListOffsetPartitionResponse getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new ListOffsetPartitionResponse(SCHEMAS[version]);
    }

    public Int32 partitionId() {
        return partitionId;
    }

    public Int16 errorCode() {
        return errorCode;
    }

    public Int64 timestamp() {
        return timestamp;
    }

    public Int64 offset() {
        return offset;
    }

    public Int32 leaderEpoch() {
        return leaderEpoch;
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(PARTITION_INDEX, partitionId);
        fieldValueBindings.put(ERROR_CODE, errorCode);
        fieldValueBindings.put(TIMESTAMP, timestamp);
        fieldValueBindings.put(OFFSET, offset);
        fieldValueBindings.put(LEADER_EPOCH, leaderEpoch);
    }

    private ListOffsetPartitionResponse(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class ListOffsetPartitionResponseFactory implements ValueFactory<ListOffsetPartitionResponse> {

        private final short version;

        public ListOffsetPartitionResponseFactory(final short version) {
            this.version = version;
        }

        @Override
        public ListOffsetPartitionResponse createInstance() {
            return ListOffsetPartitionResponse.getInstance(version);
        }
    }
}