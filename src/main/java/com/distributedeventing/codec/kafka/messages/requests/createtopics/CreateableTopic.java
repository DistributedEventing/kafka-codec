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

import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.values.*;

public final class CreateableTopic extends CompositeValue {

    /** The name of the topic to create **/
    public static final Field NAME = new Field("name");

    /**
     * The number of partitions to create in the topic or -1 if a manual
     * partition assignment is being specified or the broker should use
     * the default number of partitions.
     */
    public static final Field NUM_PARTITIONS = new Field("num_partitions");

    /**
     * The number of replicas to create per partition or -1 if a manual
     * partition assignment is being specified or the broker should use
     * the default replication factor.
     */
    public static final Field REPLICATION_FACTOR = new Field("replication_factor");

    /**
     * The manual partition assignment or an empty array if the broker should
     * use automatic partition assignment.
     */
    public static final Field ASSIGNMENTS = new Field("assignments");

    /** Configs to set on the topics **/
    public static final Field CONFIGS = new Field("configs");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V5 = new Schema(NAME,
                                                      NUM_PARTITIONS,
                                                      REPLICATION_FACTOR,
                                                      ASSIGNMENTS,
                                                      CONFIGS,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V5};

    private final CompactString name = new CompactString();

    private final Int32 numPartitions = new Int32();

    private final Int16 replicationFactor = new Int16();

    private final CreatableReplicaAssignment.CreatableReplicaAssignmentFactory assignmentFactory;

    private final CreateableTopicConfig.CreateableTopicConfigFactory configFactory;
    {
        if (schema == SCHEMA_V5) {
            assignmentFactory = new CreatableReplicaAssignment.CreatableReplicaAssignmentFactory((short) 5);
            configFactory = new CreateableTopicConfig.CreateableTopicConfigFactory((short) 5);
        } else {
            assignmentFactory = null;
            configFactory = null;
        }
    }

    private final CompactArray<CreatableReplicaAssignment> assignments = new CompactArray<>(assignmentFactory);

    private final CompactArray<CreateableTopicConfig> configs = new CompactArray<>(configFactory);

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static CreateableTopic getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new CreateableTopic(SCHEMAS[version]);
    }

    public void name(final byte[] nameBytes) {
        name.value(nameBytes);
    }

    public void name(final byte[] nameByes,
                     final int offset,
                     final int length) {
        name.value(nameByes, offset, length);
    }

    public void numPartitions(final int partitions) {
        numPartitions.value(partitions);
    }

    public void replicationFactor(final short factor) {
        replicationFactor.value(factor);
    }

    public CreatableReplicaAssignment createAssignment() {
        final CreatableReplicaAssignment assignment = assignmentFactory.createInstance();
        assignments.add(assignment);
        return assignment;
    }

    public CreateableTopicConfig createConfig() {
        final CreateableTopicConfig config = configFactory.createInstance();
        configs.add(config);
        return config;
    }

    public void taggedFields(final TaggedBytes...taggedBytes) {
        taggedFields.value(taggedBytes);
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(NAME, name);
        fieldValueBindings.put(NUM_PARTITIONS, numPartitions);
        fieldValueBindings.put(REPLICATION_FACTOR, replicationFactor);
        fieldValueBindings.put(ASSIGNMENTS, assignments);
        fieldValueBindings.put(CONFIGS, configs);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private CreateableTopic(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class CreateableTopicFactory implements ValueFactory<CreateableTopic> {

        private final short version;

        public CreateableTopicFactory(final short version) {
            this.version = version;
        }

        @Override
        public CreateableTopic createInstance() {
            return CreateableTopic.getInstance(version);
        }
    }
}