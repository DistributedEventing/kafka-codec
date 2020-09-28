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

import com.distributedeventing.codec.kafka.exceptions.ParseException;
import com.distributedeventing.codec.kafka.messages.responses.ResponseMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.util.List;

/**
 * Decodes the bytes of a response read from a Kafka broker into a
 * {@link ResponseMessage} according to the
 * <a href="https://kafka.apache.org/protocol">Kafka wire protocol</a>.
 *
 * This decoder must be used in conjunction with a {@link LengthFieldBasedFrameDecoder}
 * since Kafka brokers use a length field to identify the end of a response
 * message. An easier thing to do is to just use a {@link PipelineInitializer}
 * that takes care of setting up an encoder and decoders for you.
 */
public final class Decoder extends ByteToMessageDecoder {

    private final CorrelationMap correlationMap;

    public Decoder(final CorrelationMap correlationMap) {
        this.correlationMap = correlationMap;
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx,
                          final ByteBuf in,
                          final List<Object> out) throws Exception {
        in.markReaderIndex();
        final int correlationId = in.readInt();
        in.resetReaderIndex();
        final ApiType apiType = correlationMap.getApiType(correlationId);
        if (apiType == null) {
            throw new ParseException(String.format("Don't know the API type for correlationId %d",
                                                   correlationId));
        }
        final ResponseMessage message = apiType.getResponseMessage();
        message.readFrom(in);
        out.add(message);
    }
}