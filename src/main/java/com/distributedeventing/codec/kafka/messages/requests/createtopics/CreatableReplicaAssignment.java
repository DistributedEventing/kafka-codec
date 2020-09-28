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
package com.distributedeventing.codec.kafka.messages.requests.createtopics;

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.*;

public final class CreatableReplicaAssignment extends CompositeValue {

    /** Index of a partition in the assignment **/
    public static final Field PARTITION_INDEX = new Field("partition_index");

    /** IDs of the brokers to put the partition on **/
    public static final Field BROKER_IDS = new Field("broker_ids");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V5 = new Schema(PARTITION_INDEX,
                                                      BROKER_IDS,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V5};

    private final Int32 partitionIndex = new Int32();

    private final CompactArray<Int32> brokerIds = new CompactArray<>(Int32.FACTORY);

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static CreatableReplicaAssignment getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new CreatableReplicaAssignment(SCHEMAS[version]);
    }

    public void partitionIndex(final int idx) {
        partitionIndex.value(idx);
    }

    public void brokerIds(final Int32...ids) {
        brokerIds.value(ids);
    }

    public void taggedFields(final TaggedBytes...taggedBytes) {
        taggedFields.value(taggedBytes);
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(PARTITION_INDEX, partitionIndex);
        fieldValueBindings.put(BROKER_IDS, brokerIds);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private CreatableReplicaAssignment(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class CreatableReplicaAssignmentFactory implements ValueFactory<CreatableReplicaAssignment> {

        private final short version;

        public CreatableReplicaAssignmentFactory(final short version) {
            this.version = version;
        }

        @Override
        public CreatableReplicaAssignment createInstance() {
            return CreatableReplicaAssignment.getInstance(version);
        }
    }
}