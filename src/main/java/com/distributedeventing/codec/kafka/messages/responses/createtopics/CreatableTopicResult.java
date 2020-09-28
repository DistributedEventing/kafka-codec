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
package com.distributedeventing.codec.kafka.messages.responses.createtopics;

import com.distributedeventing.codec.kafka.BrokerError;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.*;

public final class CreatableTopicResult extends CompositeValue {

    /** Name of the topic **/
    public static final Field NAME = new Field("name");

    /**
     * Response error code. Will be {@link BrokerError#NONE}
     * if no error occurred
     */
    public static final Field ERROR_CODE = new Field("error_code");

    /** Error message or null if there is no error **/
    public static final Field ERROR_MESSAGE = new Field("error_message");

    /** Number of partitions of the topic **/
    public static final Field NUM_PARTITIONS = new Field("num_partitions");

    /** Replication factor of the topic **/
    public static final Field REPLICATION_FACTOR = new Field("replication_factor");

    /** Configurations of the topic **/
    public static final Field CONFIGS = new Field("configs");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V5 = new Schema(NAME,
                                                      ERROR_CODE,
                                                      ERROR_MESSAGE,
                                                      NUM_PARTITIONS,
                                                      REPLICATION_FACTOR,
                                                      CONFIGS,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V5};

    private final CompactString name = new CompactString();

    private final Int16 errorCode = new Int16();

    private final CompactNullableString errorMessage = new CompactNullableString();

    private final Int32 numPartitions = new Int32();

    private final Int16 replicationFactor = new Int16();

    private final CompactArray<CreatableTopicConfigs> configs;
    {
        if (schema == SCHEMA_V5) {
            configs = new CompactArray<>(new CreatableTopicConfigs.CreatableTopicConfigsFactory((short) 5));
        } else {
            configs = null;
        }
    }

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static CreatableTopicResult getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new CreatableTopicResult(SCHEMAS[version]);
    }

    public CompactString name() {
        return name;
    }

    public Int16 errorCode() {
        return errorCode;
    }

    public CompactNullableString errorMessage() {
        return errorMessage;
    }

    public Int32 numPartitions() {
        return numPartitions;
    }

    public Int16 replicationFactor() {
        return replicationFactor;
    }

    public Array<CreatableTopicConfigs>.ElementIterator configs() {
        return configs.iterator();
    }

    public Array<TaggedBytes>.ElementIterator taggedFields() {
        return taggedFields.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(NAME, name);
        fieldValueBindings.put(ERROR_CODE, errorCode);
        fieldValueBindings.put(ERROR_MESSAGE, errorMessage);
        fieldValueBindings.put(NUM_PARTITIONS, numPartitions);
        fieldValueBindings.put(REPLICATION_FACTOR, replicationFactor);
        fieldValueBindings.put(CONFIGS, configs);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private CreatableTopicResult(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class CreatableTopicResultFactory implements ValueFactory<CreatableTopicResult> {

        private final short version;

        public CreatableTopicResultFactory(final short version) {
            this.version = version;
        }

        @Override
        public CreatableTopicResult createInstance() {
            return CreatableTopicResult.getInstance(version);
        }
    }
}