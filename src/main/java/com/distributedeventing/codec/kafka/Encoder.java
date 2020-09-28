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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToByteEncoder;
import com.distributedeventing.codec.kafka.headers.RequestHeader;
import com.distributedeventing.codec.kafka.messages.requests.RequestMessage;

/**
 * Encodes a {@link RequestMessage} to a Kafka broker according to
 * the <a href="https://kafka.apache.org/protocol">Kafka wire protocol</a>.
 *
 * You can instantiate this directly while setting up the {@link ChannelPipeline}
 * of a {@link Channel}. But then you need to be careful to use the same
 * {@link CorrelationMap} that the corresponding {@link Decoder} uses.
 * An easier thing to do is to just use a {@link PipelineInitializer} that takes
 * care of setting up an {@link Encoder} and a {@link Decoder} in the pipeline.
 */
public class Encoder extends MessageToByteEncoder<RequestMessage>  {

    private final CorrelationMap correlationMap;

    private final byte[] clientId;

    public Encoder(final CorrelationMap correlationMap,
                   final byte[] clientId) {
        this.correlationMap = correlationMap;
        this.clientId = clientId.clone();
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx,
                          final RequestMessage msg,
                          final ByteBuf out) throws Exception {
        final RequestHeader requestHeader = msg.header();
        final int correlationId = correlationMap.getNextCorrelationId(msg.apiType());
        requestHeader.correlationId(correlationId);
        requestHeader.clientId(clientId);
        final int msgSize = msg.sizeInBytes();
        out.ensureWritable(4 + msgSize);
        out.writeInt(msgSize);
        msg.writeTo(out);
    }
}