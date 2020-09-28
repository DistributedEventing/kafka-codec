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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * A {@link ChannelInitializer} that takes care of setting up an {@link Encoder}
 * and a {@link Decoder} in the channel pipeline. Since Kafka responses have a
 * length field embedded in them that marks the end of a response, this class
 * needs a {@link LengthFieldBasedFrameDecoder} to properly decode a Kafka
 * response.
 */
public class PipelineInitializer extends ChannelInitializer<SocketChannel> {

    private final LengthFieldBasedFrameDecoder frameDecoder;

    private final TerminalHandler terminalHandler;

    private final byte[] clientId;

    public PipelineInitializer(final int maxFrameLength,
                               final TerminalHandler terminalHandler,
                               final byte[] clientId) {
        this (new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4),
              terminalHandler,
              clientId);
    }

    public PipelineInitializer(final LengthFieldBasedFrameDecoder frameDecoder,
                               final TerminalHandler terminalHandler,
                               final byte[] clientId) {
        this.frameDecoder = frameDecoder;
        this.terminalHandler = terminalHandler;
        this.clientId = clientId.clone();
    }

    @Override
    protected void initChannel(final SocketChannel channel) throws Exception {
        final ChannelPipeline pipeline = channel.pipeline();
        final CorrelationMap correlationMap = new CorrelationMap();
        pipeline.addLast(new Encoder(correlationMap, clientId));
        pipeline.addLast(frameDecoder);
        pipeline.addLast(new Decoder(correlationMap));
        pipeline.addLast(terminalHandler);
    }
}