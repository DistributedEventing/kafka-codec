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
package com.distributedeventing.codec.kafka.messages.requests.listoffset;

import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.headers.RequestHeader;
import com.distributedeventing.codec.kafka.messages.requests.RequestMessage;
import com.distributedeventing.codec.kafka.ApiType;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.messages.requests.ConsumerDefaults;
import com.distributedeventing.codec.kafka.values.Array;
import com.distributedeventing.codec.kafka.values.Int32;
import com.distributedeventing.codec.kafka.values.Int8;

/**
 * A request to a broker to fetch offset for a topic partition. This
 * request must be directed to the leader of a topic partition.
 *
 * Depending upon the timestamp provided in the input, either the
 * latest offset or the earliest available offset is returned.
 */
public final class ListOffsetRequest extends RequestMessage {

    /**
     * Broker ID of the replica that is sending the request. For consumers,
     * this should be set to {@link ConsumerDefaults#CONSUMER_REPLICA_ID}.
     */
    public static final Field REPLICA_ID = new Field("replica_id");

    /**
     * Isolation level of the request. See {@link ConsumerDefaults.IsolationLevel}.
     */
    public static final Field ISOLATION_LEVEL = new Field("isolation_level");

    /** The topics to fetch offsets for **/
    public static final Field TOPICS = new Field("topics");

    public static final Schema SCHEMA_V5 = new Schema(REPLICA_ID,
                                                      ISOLATION_LEVEL,
                                                      TOPICS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V5};

    private final Int32 replicaId = new Int32();

    private final Int8 isolationLevel = new Int8();

    private final ListOffsetTopic.ListOffsetTopicFactory topicFactory;
    {
        if (schema == SCHEMA_V5) {
            topicFactory = new ListOffsetTopic.ListOffsetTopicFactory((short) 4);
        } else {
            topicFactory = null;
        }
    }

    private final Array<ListOffsetTopic> topics = new Array<>(topicFactory);

    public static ListOffsetRequest getInstance(final int version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        final short ver = (short) version;
        final ApiType apiType = new ApiType.ListOffset((short) version);
        return new ListOffsetRequest(createRequestHeader(apiType),
                                     SCHEMAS[ver],
                                     apiType);
    }

    private static RequestHeader createRequestHeader(final ApiType apiType) {
        final short headerVersion = (short) 1;
        final RequestHeader header = RequestHeader.getInstance(headerVersion);
        header.requestApiKey(apiType.key().key());
        header.requestApiVersion(apiType.version());
        return header;
    }

    public void replicaId(final int id) {
        replicaId.value(id);
    }

    public void isolationLevel(final ConsumerDefaults.IsolationLevel level) {
        isolationLevel.value(level.level());
    }

    public ListOffsetTopic createTopic() {
        final ListOffsetTopic topic = topicFactory.createInstance();
        topics.add(topic);
        return topic;
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(REPLICA_ID, replicaId);
        fieldValueBindings.put(ISOLATION_LEVEL, isolationLevel);
        fieldValueBindings.put(TOPICS, topics);
    }

    private void setDefaults() {
        replicaId.value(ConsumerDefaults.CONSUMER_REPLICA_ID);
    }

    private ListOffsetRequest(final RequestHeader header,
                              final Schema schema,
                              final ApiType apiType) {
        super(header,
              schema,
              apiType);
        setFieldValueBindings();
        setDefaults();
    }
}