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
package com.distributedeventing.codec.kafka.messages.responses.produce;

import com.distributedeventing.codec.kafka.BrokerError;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.*;

public final class PartitionProduceResponse extends CompositeValue {

    /** Index of the partition to which produce request was sent **/
    public static final Field PARTITION_INDEX = new Field("partition_index");

    /**
     * Response error code. Will be {@link BrokerError#NONE}
     * if no error occurred
     */
    public static final Field ERROR_CODE = new Field("error_code");

    /** Base offset of the appended records **/
    public static final Field BASE_OFFSET = new Field("base_offset");

    /**
     * Timestamp of records after appending them to logs. If log append time is used for
     * the topic, the returned value will be broker's local time when it appended the
     * records. Otherwise (i.e. if create time is used), this will be -1.
     */
    public static final Field LOG_APPEND_TIME_MS = new Field("log_append_time_ms");

    public static final Field LOG_START_OFFSET = new Field("log_start_offset");

    /**
     * Errors associated with record batches, if they are dropped.
     */
    public static final Field RECORD_ERRORS = new Field("record_errors");

    /**
     * An error message that summarizes the problem with records that led to
     * dropping the batch.
     */
    public static final Field ERROR_MESSAGE = new Field("error_message");

    public static final Schema SCHEMA_V8 = new Schema(PARTITION_INDEX,
                                                      ERROR_CODE,
                                                      BASE_OFFSET,
                                                      LOG_APPEND_TIME_MS,
                                                      LOG_START_OFFSET,
                                                      RECORD_ERRORS,
                                                      ERROR_MESSAGE);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V8};

    private final Int32 partitionIndex = new Int32();

    private final Int16 errorCode = new Int16();

    private final Int64 baseOffset = new Int64();

    private final Int64 logAppendTimeMs = new Int64();

    private final Int64 logStartOffset = new Int64();

    private final Array<BatchIndexAndErrorMessage> recordErrors;
    {
        if (schema == SCHEMA_V8) {
            recordErrors = new Array<>(new BatchIndexAndErrorMessage.BatchIndexAndErrorMessageFactory((short) 8));
        } else {
            recordErrors = null;
        }
    }

    private final NullableString errorMessage = new NullableString();

    public static PartitionProduceResponse getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new PartitionProduceResponse(SCHEMAS[version]);
    }

    public Int32 partitionIndex() {
        return partitionIndex;
    }

    public Int16 errorCode() {
        return errorCode;
    }

    public Int64 baseOffset() {
        return baseOffset;
    }

    public Int64 logAppendTimeMs() {
        return logAppendTimeMs;
    }

    public Int64 logStartOffset() {
        return logStartOffset;
    }

    public Array<BatchIndexAndErrorMessage>.ElementIterator recordErrors() {
        return recordErrors.iterator();
    }

    public NullableString errorMessage() {
        return errorMessage;
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(PARTITION_INDEX, partitionIndex);
        fieldValueBindings.put(ERROR_CODE, errorCode);
        fieldValueBindings.put(BASE_OFFSET, baseOffset);
        fieldValueBindings.put(LOG_APPEND_TIME_MS, logAppendTimeMs);
        fieldValueBindings.put(LOG_START_OFFSET, logStartOffset);
        fieldValueBindings.put(RECORD_ERRORS, recordErrors);
        fieldValueBindings.put(ERROR_MESSAGE, errorMessage);
    }

    private PartitionProduceResponse(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class PartitionProduceResponseFactory implements ValueFactory<PartitionProduceResponse> {

        private final short version;

        public PartitionProduceResponseFactory(final short version) {
            this.version = version;
        }

        @Override
        public PartitionProduceResponse createInstance() {
            return PartitionProduceResponse.getInstance(version);
        }
    }
}