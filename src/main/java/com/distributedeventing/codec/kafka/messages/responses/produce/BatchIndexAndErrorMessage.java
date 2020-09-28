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

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.CompositeValue;
import com.distributedeventing.codec.kafka.values.Int32;
import com.distributedeventing.codec.kafka.values.NullableString;
import com.distributedeventing.codec.kafka.values.ValueFactory;

public final class BatchIndexAndErrorMessage extends CompositeValue {

    /**
     * Index of the record in the batch that caused the batch to be
     * dropped.
     */
    public static final Field BATCH_INDEX = new Field("batch_index");

    /**
     * Error associated with the record that caused the batch to be
     * dropped.
     */
    public static final Field BATCH_INDEX_ERROR_MESSAGE = new Field("batch_index_error_message");

    public static final Schema SCHEMA_V8 = new Schema(BATCH_INDEX,
                                                      BATCH_INDEX_ERROR_MESSAGE);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V8};

    private final Int32 batchIndex = new Int32();

    private final NullableString batchIndexErrorMessage = new NullableString();

    public static BatchIndexAndErrorMessage getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new BatchIndexAndErrorMessage(SCHEMAS[version]);
    }

    public Int32 batchIndex() {
        return batchIndex;
    }

    public NullableString batchIndexErrorMessage() {
        return batchIndexErrorMessage;
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(BATCH_INDEX, batchIndex);
        fieldValueBindings.put(BATCH_INDEX_ERROR_MESSAGE, batchIndexErrorMessage);
    }

    private BatchIndexAndErrorMessage(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class BatchIndexAndErrorMessageFactory implements ValueFactory<BatchIndexAndErrorMessage> {

        private final short version;

        public BatchIndexAndErrorMessageFactory(final short version) {
            this.version = version;
        }

        @Override
        public BatchIndexAndErrorMessage createInstance() {
            return BatchIndexAndErrorMessage.getInstance(version);
        }
    }
}