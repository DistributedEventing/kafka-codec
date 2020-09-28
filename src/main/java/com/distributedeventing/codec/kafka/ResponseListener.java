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

import com.distributedeventing.codec.kafka.messages.responses.apiversions.ApiVersionsResponse;
import com.distributedeventing.codec.kafka.messages.responses.createtopics.CreateTopicsResponse;
import com.distributedeventing.codec.kafka.messages.responses.deletetopics.DeleteTopicsResponse;
import com.distributedeventing.codec.kafka.messages.responses.fetch.FetchResponse;
import com.distributedeventing.codec.kafka.messages.responses.listoffset.ListOffsetResponse;
import com.distributedeventing.codec.kafka.messages.responses.metadata.MetadataResponse;
import com.distributedeventing.codec.kafka.messages.responses.produce.ProduceResponse;
import io.netty.channel.Channel;

/**
 * An interface for an object that will be called when a response is obtained
 * from a Kafka broker. Implementations of this must be registered with a
 * {@link TerminalHandler} that has been setup in the channel pipeline.
 */
public interface ResponseListener {

    void onMetadataResponse(MetadataResponse response, Channel channel);

    void onApiVersionsResponse(ApiVersionsResponse response, Channel channel);

    void onProduceResponse(ProduceResponse response, Channel channel);

    void onFetchResponse(FetchResponse response, Channel channel);

    void onCreateTopicsResponse(CreateTopicsResponse response, Channel channel);

    void onDeleteTopicsResponse(DeleteTopicsResponse response, Channel channel);

    void onListOffsetsResponse(ListOffsetResponse response, Channel channel);
}