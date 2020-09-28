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
package com.distributedeventing.codec.kafka.messages.requests;

import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.headers.RequestHeader;
import com.distributedeventing.codec.kafka.values.*;
import com.distributedeventing.codec.kafka.ApiType;
import com.distributedeventing.codec.kafka.Field;

/**
 * A request to a Kafka broker to delete one or more topics. Delete topics requests
 * must be sent to the controller of the cluster. One must be authorized to delete
 * a topic.
 *
 * Delete requests are not transactional i.e. if an error occurs for one topic, other
 * topics could still be deleted.
 */
public final class DeleteTopicsRequest extends RequestMessage {

    /** The topics to be deleted **/
    public static final Field TOPIC_NAMES = new Field("topic_names");

    /**
     * The interval (in ms) that the broker should wait before sending a response.
     * A timeout greater than 0 will cause the controller to block until either
     * the delete is complete and the controller has updated the metadata or a timeout
     * occurs. If the request times out, it does not imply that it has failed. Rather,
     * it implies that the controller could not finish the operation by then. User
     * will have to query the updated metadata to find the result of operation.
     *
     * Setting timeout to <= 0 causes the controller to validate the arguments, initiate
     * a delete, and then return immediately. This is the fully asynchronous mode.
     */
    public static final Field TIMEOUT_MS = new Field("timeout_ms");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V4 = new Schema(TOPIC_NAMES,
                                                      TIMEOUT_MS,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V4};

    private final CompactArray<CompactString> topicNames = new CompactArray<>(new CompactString.CompactStringFactory());

    private final Int32 timeoutMs = new Int32();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static DeleteTopicsRequest getInstance(final int version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        final short ver = (short) version;
        final ApiType apiType = new ApiType.DeleteTopics(ver);
        return new DeleteTopicsRequest(createRequestHeader(apiType),
                                       SCHEMAS[ver],
                                       apiType);
    }

    private static RequestHeader createRequestHeader(final ApiType apiType) {
        final short headerVersion = (short) (apiType.version() >= 4? 2: 1);
        final RequestHeader header = RequestHeader.getInstance(headerVersion);
        header.requestApiKey(apiType.key().key());
        header.requestApiVersion(apiType.version());
        return header;
    }

    public void topicNames(final CompactString...names) {
        topicNames.value(names);
    }

    public void timeoutMs(final int interval) {
        timeoutMs.value(interval);
    }

    public void taggedFields(final TaggedBytes...taggedBytes) {
        taggedFields.value(taggedBytes);
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(TOPIC_NAMES, topicNames);
        fieldValueBindings.put(TIMEOUT_MS, timeoutMs);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private DeleteTopicsRequest(final RequestHeader header,
                                final Schema schema,
                                final ApiType apiType) {
        super(header,
              schema,
              apiType);
        setFieldValueBindings();
    }
}