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
package com.distributedeventing.codec.kafka.messages.requests.metadata;

import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.headers.RequestHeader;
import com.distributedeventing.codec.kafka.messages.requests.RequestMessage;
import com.distributedeventing.codec.kafka.messages.responses.metadata.MetadataResponse;
import com.distributedeventing.codec.kafka.values.TaggedBytes;
import com.distributedeventing.codec.kafka.ApiType;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.values.Boolean;
import com.distributedeventing.codec.kafka.values.CompactNullableArray;
import com.distributedeventing.codec.kafka.values.UvilArray;

/**
 * A metadata request to a Kafka broker. This may be sent to any broker in the cluster.
 * Metadata requests are used to obtain information about a Kafka cluster, such as
 * clusterId, controllerId, brokers in the cluster, topics, partitions, etc. For more
 * information see {@link MetadataResponse}.
 */
public final class MetadataRequest extends RequestMessage {

    /**
     * The topics to fetch metadata for. In version 0, an empty array
     * is used to indicate all topics. In versions 1 and higher, an empty
     * array indicates no topics while a null array indicates all topics.
     */
    public static final Field TOPICS = new Field("topics");

    /**
     * Whether a Kafka broker should automatically create a topic for which it
     * has received a metadata request but which does not exist yet. A broker
     * might not automatically create a topic even if this is set to true if
     * the broker has not been configured to auto create topics.
     */
    public static final Field ALLOW_AUTO_TOPIC_CREATION = new Field("allow_auto_topic_creation");

    /** Whether the brokers should include cluster authorized operations in metadata response **/
    public static final Field INCLUDE_CLUSTER_AUTHORIZED_OPERATIONS = new Field("include_cluster_authorized_operations");

    /** Whether the brokers should include topic authorized operations in metadata response **/
    public static final Field INCLUDE_TOPIC_AUTHORIZED_OPERATIONS = new Field("include_topic_authorized_operations");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V9 = new Schema(TOPICS,
                                                      ALLOW_AUTO_TOPIC_CREATION,
                                                      INCLUDE_CLUSTER_AUTHORIZED_OPERATIONS,
                                                      INCLUDE_TOPIC_AUTHORIZED_OPERATIONS,
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

    private final MetadataRequestTopic.MetadataRequestTopicFactory topicFactory;
    {
        if (schema == SCHEMA_V9) {
            topicFactory = new MetadataRequestTopic.MetadataRequestTopicFactory((short) 9);
        } else {
            topicFactory = null;
        }
    }

    private final CompactNullableArray<MetadataRequestTopic> topics = new CompactNullableArray<>(topicFactory);

    private final Boolean allowAutoTopicCreation = new Boolean();

    private final Boolean includeClusterAuthorizedOperations = new Boolean();

    private final Boolean includeTopicAuthorizedOperations = new Boolean();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static MetadataRequest getInstance(final int version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        final short ver = (short) version;
        final ApiType apiType = new ApiType.Metadata(ver);
        return new MetadataRequest(createRequestHeader(apiType),
                                   SCHEMAS[ver],
                                   apiType);
    }

    private static RequestHeader createRequestHeader(final ApiType apiType) {
        final short headerVersion = (short)  (apiType.version() >= 9? 2: 1);
        final RequestHeader header = RequestHeader.getInstance(headerVersion);
        header.requestApiKey(apiType.key().key());
        header.requestApiVersion(apiType.version());
        return header;
    }

    public MetadataRequestTopic createTopic() {
        final MetadataRequestTopic topic = topicFactory.createInstance();
        topics.add(topic);
        return topic;
    }

    public void nullifyTopics() {
        topics.nullify();
    }

    public void emptyTopics() {
        topics.empty();
    }

    public void allowAutoTopicCreation(final boolean allow) {
        allowAutoTopicCreation.value(allow);
    }

    public void includeClusterAuthorizedOperations(final boolean include) {
        includeClusterAuthorizedOperations.value(include);
    }

    public void includeTopicAuthorizedOperations(final boolean include) {
        includeTopicAuthorizedOperations.value(include);
    }

    public void taggedFields(final TaggedBytes...taggedBytes) {
        taggedFields.value(taggedBytes);
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(TOPICS, topics);
        fieldValueBindings.put(ALLOW_AUTO_TOPIC_CREATION, allowAutoTopicCreation);
        fieldValueBindings.put(INCLUDE_CLUSTER_AUTHORIZED_OPERATIONS, includeClusterAuthorizedOperations);
        fieldValueBindings.put(INCLUDE_TOPIC_AUTHORIZED_OPERATIONS, includeTopicAuthorizedOperations);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private MetadataRequest(final RequestHeader header,
                            final Schema schema,
                            final ApiType apiType) {
        super(header,
              schema,
              apiType);
        setFieldValueBindings();
    }
}