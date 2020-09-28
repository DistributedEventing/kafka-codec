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
import com.distributedeventing.codec.kafka.values.*;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.values.String;

public final class ForgottenTopic extends CompositeValue {

    /** Name of the topic to remove from incremental fetch requests **/
    public static final Field NAME = new Field("name");

    /**
     * Indexes of the topic partitions to remove from incremental
     * fetch requests.
     */
    public static final Field FORGOTTEN_PARTITION_INDEXES = new Field("forgotten_partition_indexes");

    public static final Schema SCHEMA_V7 = new Schema(NAME,
                                                      FORGOTTEN_PARTITION_INDEXES);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V7};

    private final String name = new String();

    private final Array<Int32> forgottenPartitionIndexes = new Array<>(Int32.FACTORY);

    public static ForgottenTopic getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new ForgottenTopic(SCHEMAS[version]);
    }

    public void name(final byte[] nameBytes,
                     final int offset,
                     final int length) {
        name.value(nameBytes, offset, length);
    }

    public void forgottenPartitionIndexes(final Int32...ids) {
        forgottenPartitionIndexes.value(ids);
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(NAME, name);
        fieldValueBindings.put(FORGOTTEN_PARTITION_INDEXES, forgottenPartitionIndexes);
    }

    private ForgottenTopic(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class ForgottenTopicFactory implements ValueFactory<ForgottenTopic> {

        private final short version;

        public ForgottenTopicFactory(final short version) {
            this.version = version;
        }

        @Override
        public ForgottenTopic createInstance() {
            return ForgottenTopic.getInstance(version);
        }
    }
}