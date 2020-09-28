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

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.CompositeValue;
import com.distributedeventing.codec.kafka.values.Int64;
import com.distributedeventing.codec.kafka.values.ValueFactory;

public final class AbortedTransaction extends CompositeValue {

    /** ID of the producer of the aborted transaction **/
    public static final Field PRODUCER_ID = new Field("producer_id");

    /** First offset in the aborted transaction **/
    public static final Field FIRST_OFFSET = new Field("first_offset");

    public static final Schema SCHEMA_V11 = new Schema(PRODUCER_ID,
                                                       FIRST_OFFSET);

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

    private final Int64 producerId = new Int64();

    private final Int64 firstOffset = new Int64();

    public static AbortedTransaction getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new AbortedTransaction(SCHEMAS[version]);
    }

    public Int64 producerId() {
        return producerId;
    }

    public Int64 firstOffset() {
        return firstOffset;
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(PRODUCER_ID, producerId);
        fieldValueBindings.put(FIRST_OFFSET, firstOffset);
    }

    private AbortedTransaction(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class AbortedTransactionFactory implements ValueFactory<AbortedTransaction> {

        private final short version;

        public AbortedTransactionFactory(final short version) {
            this.version = version;
        }

        @Override
        public AbortedTransaction createInstance() {
            return AbortedTransaction.getInstance(version);
        }
    }
}