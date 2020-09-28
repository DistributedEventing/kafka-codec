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
import com.distributedeventing.codec.kafka.headers.RequestHeader;
import com.distributedeventing.codec.kafka.messages.requests.RequestMessage;
import com.distributedeventing.codec.kafka.values.String;
import com.distributedeventing.codec.kafka.ApiType;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.messages.requests.ConsumerDefaults;
import com.distributedeventing.codec.kafka.values.Array;
import com.distributedeventing.codec.kafka.values.Int32;
import com.distributedeventing.codec.kafka.values.Int8;

/**
 * A request to fetch messages from Kafka. Fetch requests must be
 * directed to the leader of the topic partition to fetch messages
 * from.
 */
public final class FetchRequest extends RequestMessage {

    /**
     * Broker ID of the replica that is sending the request. For consumers,
     * this should be set to {@link ConsumerDefaults#CONSUMER_REPLICA_ID}.
     */
    public static final Field REPLICA_ID = new Field("replica_id");

    /**
     * Maximum time (in milliseconds) that the broker should wait before it
     * sends a response. If the broker times out and sends a response, it
     * does not imply that an error has occurred.
     */
    public static final Field MAX_WAIT = new Field("max_wait");

    /** Minimum bytes to accumulate before sending a response **/
    public static final Field MIN_BYTES = new Field("min_bytes");

    /**
     * Maximum bytes to fetch in the response. This may not be honored
     * under few circumstances, for example if the record size is larger
     * than this value.
     */
    public static final Field MAX_BYTES = new Field("max_bytes");

    /**
     * Isolation level of the fetch request. See {@link ConsumerDefaults.IsolationLevel}.
     * It does not matter what this field is set to if transactions are
     * not being used.
     */
    public static final Field ISOLATION_LEVEL = new Field("isolation_level");

    /**
     * The current fetch session ID or {@link FetchRequest#NO_SESSION_ID} if
     * fetch sessions are not being used.
     */
    public static final Field SESSION_ID = new Field("session_id");

    /**
     * The current fetch session epoch (if fetch sessions are being used).
     * Valid epochs start at 1, increment per fetch request, and go up to
     * {@link Integer#MAX_VALUE}. They wrap around to 1 after that.
     *
     * This could be set to an invalid value
     * (such as {@link FetchRequest#NO_SESSION_EPOCH}) if fetch sessions
     * are not being used.
     */
    public static final Field EPOCH = new Field("epoch");

    /** The topics to fetch **/
    public static final Field FETCHABLE_TOPICS = new Field("topics");

    /**
     * If fetch sessions are being used, this field can be supplied to
     * let the broker know about the topic partitions to exclude from
     * responses.
     */
    public static final Field FORGOTTEN_TOPICS = new Field("forgotten");

    /** Rack ID of the consumer making this request **/
    public static final Field RACK_ID = new Field("rack_id");

    /**
     * Session ID to set on the fetch request when fetch sessions are
     * not being used.
     */
    public static final int NO_SESSION_ID = 0;

    /**
     * Session epoch to set on the fetch request when fetch sessions are
     * not being used.
     */
    public static final int NO_SESSION_EPOCH = -1;

    public static final Schema SCHEMA_V11 = new Schema(REPLICA_ID,
                                                       MAX_WAIT,
                                                       MIN_BYTES,
                                                       MAX_BYTES,
                                                       ISOLATION_LEVEL,
                                                       SESSION_ID,
                                                       EPOCH,
                                                       FETCHABLE_TOPICS,
                                                       FORGOTTEN_TOPICS,
                                                       RACK_ID);

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

    private final Int32 replicaId = new Int32();

    private final Int32 maxWait = new Int32();

    private final Int32 minBytes = new Int32();

    private final Int32 maxBytes = new Int32();

    private final Int8 isolationLevel = new Int8();

    private final Int32 sessionId = new Int32();

    private final Int32 epoch = new Int32();

    private final FetchableTopic.FetchableTopicFactory fetchableTopicFactory;

    private final ForgottenTopic.ForgottenTopicFactory forgottenTopicFactory;
    {
        if (schema == SCHEMA_V11) {
            fetchableTopicFactory = new FetchableTopic.FetchableTopicFactory((short) 9);
            forgottenTopicFactory = new ForgottenTopic.ForgottenTopicFactory((short) 7);
        } else {
            fetchableTopicFactory = null;
            forgottenTopicFactory = null;
        }
    }

    private final Array<FetchableTopic> fetchableTopics = new Array<>(fetchableTopicFactory);

    private final Array<ForgottenTopic> forgottenTopics = new Array<>(forgottenTopicFactory);

    private final String rackId = new String();

    public static FetchRequest getInstance(final int version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        final short ver = (short) version;
        final ApiType apiType = new ApiType.Fetch(ver);
        return new FetchRequest(createRequestHeader(apiType),
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

    public void maxWait(final int interval) {
        maxWait.value(interval);
    }

    public void minBytes(final int bytes) {
        minBytes.value(bytes);
    }

    public void maxBytes(final int bytes) {
        maxBytes.value(bytes);
    }

    public void isolationLevel(final ConsumerDefaults.IsolationLevel iLevel) {
        isolationLevel.value(iLevel.level());
    }

    public void sessionId(final int id) {
        sessionId.value(id);
    }

    public void epoch(final int epoch) {
        this.epoch.value(epoch);
    }

    public FetchableTopic createFetchableTopic() {
        final FetchableTopic fetchableTopic = fetchableTopicFactory.createInstance();
        fetchableTopics.add(fetchableTopic);
        return fetchableTopic;
    }

    public ForgottenTopic createForgottenTopic() {
        final ForgottenTopic forgottenTopic = forgottenTopicFactory.createInstance();
        forgottenTopics.add(forgottenTopic);
        return forgottenTopic;
    }

    public void rackId(final byte[] rackIdBytes,
                       final int offset,
                       final int length) {
        rackId.value(rackIdBytes, offset, length);
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(REPLICA_ID, replicaId);
        fieldValueBindings.put(MAX_WAIT, maxWait);
        fieldValueBindings.put(MIN_BYTES, minBytes);
        fieldValueBindings.put(MAX_BYTES, maxBytes);
        fieldValueBindings.put(ISOLATION_LEVEL, isolationLevel);
        fieldValueBindings.put(SESSION_ID, sessionId);
        fieldValueBindings.put(EPOCH, epoch);
        fieldValueBindings.put(FETCHABLE_TOPICS, fetchableTopics);
        fieldValueBindings.put(FORGOTTEN_TOPICS, forgottenTopics);
        fieldValueBindings.put(RACK_ID, rackId);
    }

    private void setDefaults() {
        replicaId.value(ConsumerDefaults.CONSUMER_REPLICA_ID); // consumers are not replicas
        sessionId.value(NO_SESSION_ID); // no fetch session
        epoch.value(NO_SESSION_EPOCH); // no fetch session
    }

    private FetchRequest(final RequestHeader header,
                         final Schema schema,
                         final ApiType apiType) {
        super(header,
              schema,
              apiType);
        setFieldValueBindings();
        setDefaults();
    }
}