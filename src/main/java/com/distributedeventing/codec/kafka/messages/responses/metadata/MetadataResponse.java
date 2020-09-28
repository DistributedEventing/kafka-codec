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

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.messages.requests.metadata.MetadataRequest;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.headers.ResponseHeader;
import com.distributedeventing.codec.kafka.messages.responses.ResponseMessage;
import com.distributedeventing.codec.kafka.values.*;

public final class MetadataResponse extends ResponseMessage {

    /**
     * The amount of time for which the metadata request was throttled due to a quota
     * violation. This will be 0 if there was no violation.
     */
    public static final Field THROTTLE_TIME_MS = new Field("throttle_time_ms");

    /** Current set of brokers in the Kafka cluster. **/
    public static final Field BROKERS = new Field("brokers");

    /** The identifier of the Kafka cluster **/
    public static final Field CLUSTER_ID = new Field("cluster_id");

    /** The identifier of the controller broker **/
    public static final Field CONTROLLER_ID = new Field("controller_id");

    /**
     * An array of topics. This will depend upon the topics one has requested for,
     * see {@link MetadataRequest#TOPICS}.
     */
    public static final Field TOPICS = new Field("topics");

    /** Bit vector that represents authorized operations on the cluster **/
    public static final Field CLUSTER_AUTHORIZED_OPERATIONS = new Field("cluster_authorized_operations");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V9 = new Schema(THROTTLE_TIME_MS,
                                                      BROKERS,
                                                      CLUSTER_ID,
                                                      CONTROLLER_ID,
                                                      TOPICS,
                                                      CLUSTER_AUTHORIZED_OPERATIONS,
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

    private final Int32 throttleTimeMs = new Int32();

    private final CompactArray<MetadataResponseBroker> brokers;

    private final CompactNullableString clusterId = new CompactNullableString();

    private final Int32 controlledId = new Int32();

    private final CompactArray<MetadataResponseTopic> topics;
    {
        if (schema == SCHEMA_V9) {
            brokers = new CompactArray<>(new MetadataResponseBroker.MetadataResponseBrokerFactory((short) 9));
            topics = new CompactArray<>(new MetadataResponseTopic.MetadataResponseTopicFactory((short) 9));
        } else {
            brokers = null;
            topics = null;
        }
    }

    private final Int32 clusterAuthorizedOperations = new Int32();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static MetadataResponse getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new MetadataResponse(createResponseHeader(version),
                                    SCHEMAS[version]);
    }

    private static ResponseHeader createResponseHeader(final short version) {
        final short headerVersion = (short) (version >= 9? 1: 0);
        return ResponseHeader.getInstance(headerVersion);
    }

    public Int32 throttleTimeMs() {
        return throttleTimeMs;
    }

    public Array<MetadataResponseBroker>.ElementIterator brokers() {
        return brokers.iterator();
    }

    public CompactNullableString clusterId() {
        return clusterId;
    }

    public Int32 controlledId() {
        return controlledId;
    }

    public Array<MetadataResponseTopic>.ElementIterator topics() {
        return topics.iterator();
    }

    public Int32 clusterAuthorizedOperations() {
        return clusterAuthorizedOperations;
    }

    public Array<TaggedBytes>.ElementIterator taggedFields() {
        return taggedFields.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(THROTTLE_TIME_MS, throttleTimeMs);
        fieldValueBindings.put(BROKERS, brokers);
        fieldValueBindings.put(CLUSTER_ID, clusterId);
        fieldValueBindings.put(CONTROLLER_ID, controlledId);
        fieldValueBindings.put(TOPICS, topics);
        fieldValueBindings.put(CLUSTER_AUTHORIZED_OPERATIONS, clusterAuthorizedOperations);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private MetadataResponse(final ResponseHeader header,
                             final Schema schema) {
        super(header,
              schema);
        setFieldValueBindings();
    }
}