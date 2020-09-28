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
package com.distributedeventing.codec.kafka.messages.requests.produce;

import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.headers.RequestHeader;
import com.distributedeventing.codec.kafka.messages.requests.RequestMessage;
import com.distributedeventing.codec.kafka.values.NullableString;
import com.distributedeventing.codec.kafka.ApiType;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.values.Array;
import com.distributedeventing.codec.kafka.values.Int16;
import com.distributedeventing.codec.kafka.values.Int32;

/**
 * A request to produce messages to a broker. A produce request must always
 * be directed to the leader of a topic partition.
 */
public final class ProduceRequest extends RequestMessage {

    /** Producer's transactional ID or null if the producer is not transactional **/
    public static final Field TRANSACTIONAL_ID = new Field("transactional_id");

    /**
     * The number of acks from replicas that the leader should receive before
     * considering the produce request to be complete. Must be one of {@link Acks}.
     */
    public static final Field ACKS = new Field("acks");

    /**
     * The amount of time (in ms) that the broker should wait for request completion
     * before sending a response.
     */
    public static final Field TIMEOUT_MS = new Field("timeout_ms");

    /** The topics to produce to **/
    public static final Field TOPICS = new Field("topics");

    public static final Schema SCHEMA_V8 = new Schema(TRANSACTIONAL_ID,
                                                      ACKS,
                                                      TIMEOUT_MS,
                                                      TOPICS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V8};

    private final NullableString transactionalId = new NullableString();

    private final Int16 acks = new Int16();

    private final Int32 timeoutMs = new Int32();

    private final TopicProduceData.TopicProduceDataFactory topicFactory;
    {
        if (schema == SCHEMA_V8) {
            topicFactory = new TopicProduceData.TopicProduceDataFactory((short) 0);
        } else {
            topicFactory = null;
        }
    }

    private final Array<TopicProduceData> topics = new Array<>(topicFactory);

    public static ProduceRequest getInstance(final int version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        final short ver = (short) version;
        final ApiType apiType = new ApiType.Produce(ver);
        return new ProduceRequest(createRequestHeader(apiType),
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

    public void transactionalId(final byte[] idBytes) {
        transactionalId.value(idBytes);
    }

    public void transactionalId(final byte[] idBytes,
                                final int offset,
                                final int length) {
        transactionalId.value(idBytes, offset, length);
    }

    public void acks(final Acks acks) {
        this.acks.value(acks.value());
    }

    public void timeoutMs(final int interval) {
        timeoutMs.value(interval);
    }

    public TopicProduceData createTopic() {
        final TopicProduceData topic = topicFactory.createInstance();
        topics.add(topic);
        return topic;
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(TRANSACTIONAL_ID, transactionalId);
        fieldValueBindings.put(ACKS, acks);
        fieldValueBindings.put(TIMEOUT_MS, timeoutMs);
        fieldValueBindings.put(TOPICS, topics);
    }

    private ProduceRequest(final RequestHeader header,
                           final Schema schema,
                           final ApiType apiType) {
        super(header,
              schema,
              apiType);
        setFieldValueBindings();
        transactionalId.nullify();
    }

    public static enum Acks {

        /** Acks from the while in-sync replica set **/
        FULL_ISR(-1),

        /** No acks **/
        NONE(0),

        /** Ack from the leader only **/
        LEADER_ONLY(1);

        private final short value;

        public short value() {
            return value;
        }

        private Acks(final int value) {
            this.value = (short) value;
        }
    }
}