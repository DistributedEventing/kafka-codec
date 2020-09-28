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
import io.netty.channel.embedded.EmbeddedChannel;
import com.distributedeventing.codec.kafka.headers.ResponseHeader;
import com.distributedeventing.codec.kafka.messages.responses.ResponseMessage;
import com.distributedeventing.codec.kafka.messages.responses.deletetopics.DeletableTopicResult;
import com.distributedeventing.codec.kafka.messages.responses.deletetopics.DeleteTopicsResponse;
import com.distributedeventing.codec.kafka.values.Array;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

public class DecoderTest {

    private CorrelationMap correlationMap;

    private EmbeddedChannel channel;

    @Before
    public void setUp() {
        correlationMap = new CorrelationMap();
        channel = new EmbeddedChannel(new Decoder(correlationMap));
    }

    @After
    public void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    public void testDeleteTopicsResponseV4Decoding() {
        // This is only a test of the overall decoding process. Decoding of individual
        // responses has been tested separately.
        String responseHex = "00 00 00 00 00 00 00 00 00 02 05 74 65 73 74 00 00 00 00";
        ByteBuf incoming = TestUtils.hexToBinary(responseHex);
        correlationMap.getNextCorrelationId(new ApiType.DeleteTopics((short) 4));
        channel.writeInbound(incoming);
        ResponseMessage message = channel.readInbound();
        assertTrue(message instanceof DeleteTopicsResponse);
        DeleteTopicsResponse response = (DeleteTopicsResponse) message;
        ResponseHeader header = response.header();
        assertEquals(0, header.correlationId().value());
        assertFalse(header.taggedFields().hasNext());
        assertEquals(0, response.throttleTimeMs().value());
        Array<DeletableTopicResult>.ElementIterator responses = response.responses();
        assertTrue(responses.hasNext());
        DeletableTopicResult topicResult = responses.next();
        assertFalse(responses.hasNext());
        assertEquals("test", topicResult.name().valueAsString());
        assertEquals(0, topicResult.errorCode().value());
        assertFalse(topicResult.taggedFields().hasNext());
        assertFalse(response.taggedFields().hasNext());
    }
}