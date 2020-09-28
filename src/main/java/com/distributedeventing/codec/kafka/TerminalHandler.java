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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * A handler for {@link ResponseMessage}es received from Kafka. Multiple
 * {@link ResponseListener}s can be registered with this so that they are
 * invoked upon receipt of messages from Kafka.
 */
public class TerminalHandler extends ChannelInboundHandlerAdapter {

    private final List<ResponseListener> listeners = new ArrayList<>();

    public void addListener(final ResponseListener listener) {
        listeners.add(listener);
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx,
                                final Throwable cause) throws Exception {
        cause.printStackTrace();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx,
                            final Object msg) throws Exception {
        final Channel channel = ctx.channel();
        final ResponseMessage response = (ResponseMessage) msg;
        if (response instanceof ApiVersionsResponse) {
            onApiVersionsResponse((ApiVersionsResponse) response, channel);
        } else if (response instanceof CreateTopicsResponse) {
            onCreateTopicsResponse((CreateTopicsResponse) response, channel);
        } else if (response instanceof DeleteTopicsResponse) {
            onDeleteTopicsResponse((DeleteTopicsResponse) response, channel);
        } else if (response instanceof FetchResponse) {
            onFetchResponse((FetchResponse) response, channel);
        } else if (response instanceof ListOffsetResponse) {
            onListOffsetResponse((ListOffsetResponse) response, channel);
        } else if (response instanceof MetadataResponse) {
            onMetadataResponse((MetadataResponse) response, channel);
        } else if (response instanceof ProduceResponse) {
            onProduceResponse((ProduceResponse) response, channel);
        } else {
            throw new RuntimeException("Received an unknown response message type; correlationId: " +
                                       response.header().correlationId());
        }
    }

    private void onProduceResponse(final ProduceResponse response,
                                   final Channel channel) {
        for (final ResponseListener listener: listeners) {
            listener.onProduceResponse(response, channel);
        }
    }

    private void onMetadataResponse(final MetadataResponse response,
                                    final Channel channel) {
        for (final ResponseListener listener: listeners) {
            listener.onMetadataResponse(response, channel);
        }
    }

    private void onListOffsetResponse(final ListOffsetResponse response,
                                      final Channel channel) {
        for (final ResponseListener listener: listeners) {
            listener.onListOffsetsResponse(response, channel);
        }
    }

    private void onFetchResponse(final FetchResponse response,
                                 final Channel channel) {
        for (final ResponseListener listener: listeners) {
            listener.onFetchResponse(response, channel);
        }
    }

    private void onDeleteTopicsResponse(final DeleteTopicsResponse response,
                                        final Channel channel) {
        for (final ResponseListener listener: listeners) {
            listener.onDeleteTopicsResponse(response, channel);
        }
    }

    private void onCreateTopicsResponse(final CreateTopicsResponse response,
                                        final Channel channel) {
        for (final ResponseListener listener: listeners) {
            listener.onCreateTopicsResponse(response, channel);
        }
    }

    private void onApiVersionsResponse(final ApiVersionsResponse response,
                                       final Channel channel) {
        for (final ResponseListener listener: listeners) {
            listener.onApiVersionsResponse(response, channel);
        }
    }
}