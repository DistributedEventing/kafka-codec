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
package com.distributedeventing.codec.kafka.messages.responses.metadata;

import com.distributedeventing.codec.kafka.BrokerError;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.*;
import com.distributedeventing.codec.kafka.values.Boolean;

public final class MetadataResponseTopic extends CompositeValue {

    /**
     * The error code associated with the topic. Will be
     * {@link BrokerError#NONE} if no error
     * occurred.
     */
    public static final Field ERROR_CODE = new Field("error_code");

    /** Name of the topic **/
    public static final Field NAME = new Field("name");

    /** Whether the topic is internal to Kafka **/
    public static final Field IS_INTERNAL = new Field("is_internal");

    /** Partitions of the topic **/
    public static final Field PARTITIONS = new Field("partitions");

    /** 32-bit field that represents operations authrorized on this topic **/
    public static final Field TOPIC_AUTHORIZED_OPERATIONS = new Field("topic_authorized_operations");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V9 = new Schema(ERROR_CODE,
                                                      NAME,
                                                      IS_INTERNAL,
                                                      PARTITIONS,
                                                      TOPIC_AUTHORIZED_OPERATIONS,
                                                      TAGGED_FEILDS);

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

    private final Int16 errorCode = new Int16();

    private final CompactString name = new CompactString();

    private final Boolean isInternal = new Boolean();

    private final CompactArray<MetadataResponsePartition> partitions;
    {
        if (schema == SCHEMA_V9) {
            partitions = new CompactArray<>(new MetadataResponsePartition.MetadataResponsePartitionFactory((short) 9));
        } else {
            partitions = null;
        }
    }

    private final Int32 topicAuthorizedOperations = new Int32();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static MetadataResponseTopic getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new MetadataResponseTopic(SCHEMAS[version]);
    }

    public Int16 errorCode() {
        return errorCode;
    }

    public CompactString name() {
        return name;
    }

    public Boolean isInternal() {
        return isInternal;
    }

    public Array<MetadataResponsePartition>.ElementIterator partitions() {
        return partitions.iterator();
    }

    public Int32 topicAuthorizedOperations() {
        return topicAuthorizedOperations;
    }

    public Array<TaggedBytes>.ElementIterator taggedFields() {
        return taggedFields.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(ERROR_CODE, errorCode);
        fieldValueBindings.put(NAME, name);
        fieldValueBindings.put(IS_INTERNAL, isInternal);
        fieldValueBindings.put(PARTITIONS, partitions);
        fieldValueBindings.put(TOPIC_AUTHORIZED_OPERATIONS, topicAuthorizedOperations);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private MetadataResponseTopic(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class MetadataResponseTopicFactory implements ValueFactory<MetadataResponseTopic> {

        private final short version;

        public MetadataResponseTopicFactory(final short version) {
            this.version = version;
        }

        @Override
        public MetadataResponseTopic createInstance() {
            return MetadataResponseTopic.getInstance(version);
        }
    }
}