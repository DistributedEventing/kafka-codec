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
import com.distributedeventing.codec.kafka.headers.RequestHeader;
import com.distributedeventing.codec.kafka.messages.requests.RequestMessage;
import com.distributedeventing.codec.kafka.values.*;
import com.distributedeventing.codec.kafka.ApiType;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.values.Boolean;

/**
 * A request to create one or more topics. Create topics requests must be
 * sent to the controller of the cluster. To perform this operation successfully,
 * one must be authorized to create topics in the cluster.
 *
 * Creation of multiple topics with one request is not a transactional operation,
 * i.e. if the creation of one topic fails, others might still succeed.
 */
public final class CreateTopicsRequest extends RequestMessage {

    /**
     * The topics to create. Per topic, one can specify only one of the two:
     * <ul>
     *     <li>
     *         the number of partitions and the replication factor. This leads to automatic
     *         replica assignment.
     *     </li>
     *     <li>
     *         the replica assignment (as decided by user). The number of partitions and the
     *         replication factor are determined from this. The first replica in the list is
     *         taken to be the preferred leader.
     *     </li>
     * </ul>
     */
    public static final Field TOPICS = new Field("topics");

    /**
     * The interval (in ms) that the broker should wait before sending a response.
     * A timeout greater than 0 will cause the controller to block until either
     * the create is complete and the controller has updated the metadata or a timeout
     * occurs. If the request times out, it does not imply that it has failed. Rather,
     * it implies that the controller could not finish the operation by then. User
     * will have to query the updated metadata to find the result of operation.
     *
     * Setting timeout to <= 0 causes the controller to validate the arguments, initiate
     * a create, and then return immediately. This is the fully asynchronous mode.
     */
    public static final Field TIMEOUT_MS = new Field("timeout_ms");

    /**
     * If set to true, it instructs the broker to only validate whether the topics can
     * be created as specified. The broker will not actually create the topics under such
     * circumstances.
     */
    public static final Field VALIDATE_ONLY = new Field("validate_only");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V5 = new Schema(TOPICS,
                                                      TIMEOUT_MS,
                                                      VALIDATE_ONLY,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V5};

    private final CreateableTopic.CreateableTopicFactory topicFactory;
    {
        if (schema == SCHEMA_V5) {
            topicFactory = new CreateableTopic.CreateableTopicFactory((short) 5);
        } else {
            topicFactory = null;
        }
    }

    private final CompactArray<CreateableTopic> topics = new CompactArray<>(topicFactory);

    private final Int32 timeoutMs = new Int32();

    private final Boolean validateOnly = new Boolean();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static CreateTopicsRequest getInstance(final int version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        final short ver = (short) version;
        final ApiType apiType = new ApiType.CreateTopics(ver);
        return new CreateTopicsRequest(createRequestHeader(apiType),
                                       SCHEMAS[ver],
                                       apiType);
    }

    private static RequestHeader createRequestHeader(final ApiType apiType) {
        final short headerVersion = (short)  (apiType.version() >= 5? 2: 1);
        final RequestHeader header = RequestHeader.getInstance(headerVersion);
        header.requestApiKey(apiType.key().key());
        header.requestApiVersion(apiType.version());
        return header;
    }

    public CreateableTopic createTopic() {
        final CreateableTopic createableTopic = topicFactory.createInstance();
        topics.add(createableTopic);
        return createableTopic;
    }

    public void timeoutMs(final int interval) {
        timeoutMs.value(interval);
    }

    public void validateOnly(final boolean validate) {
        validateOnly.value(validate);
    }

    public void taggedFields(final TaggedBytes...taggedBytes) {
        taggedFields.value(taggedBytes);
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(TOPICS, topics);
        fieldValueBindings.put(TIMEOUT_MS, timeoutMs);
        fieldValueBindings.put(VALIDATE_ONLY, validateOnly);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private CreateTopicsRequest(final RequestHeader header,
                                final Schema schema,
                                final ApiType apiType) {
        super(header,
              schema,
              apiType);
        setFieldValueBindings();
    }
}
