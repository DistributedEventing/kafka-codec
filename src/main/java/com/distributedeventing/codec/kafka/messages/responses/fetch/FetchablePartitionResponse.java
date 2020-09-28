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

import com.distributedeventing.codec.kafka.BrokerError;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.*;
import com.distributedeventing.codec.kafka.values.records.RecordBatchArray;

public final class FetchablePartitionResponse extends CompositeValue {

    /** Index of the partition **/
    public static final Field PARTITION_INDEX = new Field("partition_index");

    /**
     * The error code associated with the topic partition. Will be
     * {@link BrokerError#NONE} if no error
     * occurred.
     */
    public static final Field ERROR_CODE = new Field("error_code");

    /**
     * The high watermark of the partition i.e. the largest offset of any
     * message that is fully replicated.
     */
    public static final Field HIGH_WATERMARK = new Field("high_watermark");

    /**
     * The last stable offset (LSO) of the topic partition. This is the last
     * offset such that all transactional records before this have been either
     * committed or aborted.
     */
    public static final Field LAST_STABLE_OFFSET = new Field("last_stable_offset");

    /**
     * The current start offset of the log. This field is important only in case
     * of inter-broker communication.
     */
    public static final Field LOG_START_OFFSET = new Field("log_start_offset");

    /** Transactions that were aborted **/
    public static final Field ABORTED = new Field("aborted");

    /** The preferred read replica for the consumer to use on its next fetch request **/
    public static final Field PREFERRED_READ_REPLICA = new Field("preferred_read_replica");

    /** The records in the response **/
    public static final Field RECORDS = new Field("records");

    public static final Schema SCHEMA_V11 = new Schema(PARTITION_INDEX,
                                                       ERROR_CODE,
                                                       HIGH_WATERMARK,
                                                       LAST_STABLE_OFFSET,
                                                       LOG_START_OFFSET,
                                                       ABORTED,
                                                       PREFERRED_READ_REPLICA,
                                                       RECORDS);

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

    private final Int32 partitionIndex = new Int32();

    private final Int16 errorCode = new Int16();

    private final Int64 highWatermark = new Int64();

    private final Int64 lastStableOffset = new Int64();

    private final Int64 logStartOffset = new Int64();

    private final NullableArray<AbortedTransaction> abortedTransactions;
    {
        if (schema == SCHEMA_V11) {
            abortedTransactions = new NullableArray<>(new AbortedTransaction.AbortedTransactionFactory((short) 4));
        } else {
            abortedTransactions = null;
        }
    }

    private final Int32 preferredReadReplica = new Int32();

    private final RecordBatchArray records = new RecordBatchArray();

    public static FetchablePartitionResponse getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new FetchablePartitionResponse(SCHEMAS[version]);
    }

    public Int32 partitionIndex() {
        return partitionIndex;
    }

    public Int16 errorCode() {
        return errorCode;
    }

    public Int64 highWatermark() {
        return highWatermark;
    }

    public Int64 lastStableOffset() {
        return lastStableOffset;
    }

    public Int64 logStartOffset() {
        return logStartOffset;
    }

    public Array<AbortedTransaction>.ElementIterator abortedTransactions() {
        return abortedTransactions.iterator();
    }

    public Int32 preferredReadReplica() {
        return preferredReadReplica;
    }

    public RecordBatchArray.ElementIterator records() {
        return records.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(PARTITION_INDEX, partitionIndex);
        fieldValueBindings.put(ERROR_CODE, errorCode);
        fieldValueBindings.put(HIGH_WATERMARK, highWatermark);
        fieldValueBindings.put(LAST_STABLE_OFFSET, lastStableOffset);
        fieldValueBindings.put(LOG_START_OFFSET, logStartOffset);
        fieldValueBindings.put(ABORTED, abortedTransactions);
        fieldValueBindings.put(PREFERRED_READ_REPLICA, preferredReadReplica);
        fieldValueBindings.put(RECORDS, records);
    }

    private FetchablePartitionResponse(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class FetchablePartitionResponseFactory implements ValueFactory<FetchablePartitionResponse> {

        private final short version;

        public FetchablePartitionResponseFactory(final short version) {
            this.version = version;
        }

        @Override
        public FetchablePartitionResponse createInstance() {
            return FetchablePartitionResponse.getInstance(version);
        }
    }
}