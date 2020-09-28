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
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.values.CompositeValue;
import com.distributedeventing.codec.kafka.values.Int32;
import com.distributedeventing.codec.kafka.values.ValueFactory;
import com.distributedeventing.codec.kafka.values.records.RecordBatch;
import com.distributedeventing.codec.kafka.values.records.RecordBatchArray;

public final class PartitionProduceData extends CompositeValue {

    /** The index of the partition to produce to **/
    public static final Field PARTITION_INDEX = new Field("partition_index");

    /** The records to produce **/
    public static final Field RECORDS = new Field("records");

    public static final Schema SCHEMA_V0 = new Schema(PARTITION_INDEX,
                                                      RECORDS);

    public static final Schema[] SCHEMAS = new Schema[] {SCHEMA_V0};

    private final Int32 partitionIndex = new Int32();

    private final RecordBatchArray records = new RecordBatchArray();

    public static PartitionProduceData getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new PartitionProduceData(version);
    }

    public void partitionIndex(final int index) {
        partitionIndex.value(index);
    }

    public RecordBatch createRecordBatch() {
        return records.createRecordBatch();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(PARTITION_INDEX, partitionIndex);
        fieldValueBindings.put(RECORDS, records);
    }

    private PartitionProduceData(final short version) {
        super(SCHEMAS[version]);
        setFieldValueBindings();
    }

    public static final class PartitionProduceDataFactory implements ValueFactory<PartitionProduceData> {

        private final short version;

        public PartitionProduceDataFactory(final short version) {
            this.version = version;
        }

        @Override
        public PartitionProduceData createInstance() {
            return PartitionProduceData.getInstance(version);
        }
    }
}