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
package com.distributedeventing.codec.kafka;

import com.distributedeventing.codec.kafka.messages.responses.ResponseMessage;
import com.distributedeventing.codec.kafka.messages.responses.apiversions.ApiVersionsResponse;
import com.distributedeventing.codec.kafka.messages.responses.createtopics.CreateTopicsResponse;
import com.distributedeventing.codec.kafka.messages.responses.deletetopics.DeleteTopicsResponse;
import com.distributedeventing.codec.kafka.messages.responses.fetch.FetchResponse;
import com.distributedeventing.codec.kafka.messages.responses.listoffset.ListOffsetResponse;
import com.distributedeventing.codec.kafka.messages.responses.metadata.MetadataResponse;
import com.distributedeventing.codec.kafka.messages.responses.produce.ProduceResponse;

public abstract class ApiType {

    public static final class ApiVersions extends ApiType {

        public ApiVersions(final short version) {
            super(ApiKey.API_VERSIONS,
                  version);
        }

        @Override
        public ResponseMessage getResponseMessage() {
            return ApiVersionsResponse.getInstance(version);
        }
    }

    public static final class CreateTopics extends ApiType {

        public CreateTopics(final short version) {
            super(ApiKey.CREATE_TOPICS,
                  version);
        }

        @Override
        public ResponseMessage getResponseMessage() {
            return CreateTopicsResponse.getInstance(version);
        }
    }

    public static final class DeleteTopics extends ApiType {

        public DeleteTopics(final short version) {
            super(ApiKey.DELETE_TOPICS,
                  version);
        }

        @Override
        public ResponseMessage getResponseMessage() {
            return DeleteTopicsResponse.getInstance(version);
        }
    }

    public static final class ListOffset extends ApiType {

        public ListOffset(final short version) {
            super(ApiKey.LIST_OFFSETS,
                  version);
        }

        @Override
        public ResponseMessage getResponseMessage() {
            return ListOffsetResponse.getInstance(version);
        }
    }

    public static final class Metadata extends ApiType {

        public Metadata(final short version) {
            super(ApiKey.METADATA,
                  version);
        }

        @Override
        public ResponseMessage getResponseMessage() {
            return MetadataResponse.getInstance(version);
        }
    }

    public static final class Produce extends ApiType {

        public Produce(final short version) {
            super(ApiKey.PRODUCE,
                  version);
        }

        @Override
        public ResponseMessage getResponseMessage() {
            return ProduceResponse.getInstance(version);
        }
    }

    public static final class Fetch extends ApiType {

        public Fetch(final short version) {
            super(ApiKey.FETCH,
                  version);
        }

        @Override
        public ResponseMessage getResponseMessage() {
            return FetchResponse.getInstance(version);
        }
    }

    protected final ApiKey key;

    protected final short version;

    public ApiKey key() {
        return key;
    }

    public short version() {
        return version;
    }

    public abstract ResponseMessage getResponseMessage();

    protected ApiType(final ApiKey key,
                      final short version) {
        this.key = key;
        this.version = version;
    }
}