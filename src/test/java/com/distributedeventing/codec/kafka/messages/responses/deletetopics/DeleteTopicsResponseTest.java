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
package com.distributedeventing.codec.kafka.messages.responses.deletetopics;

import com.distributedeventing.codec.kafka.exceptions.ParseException;
import com.distributedeventing.codec.kafka.headers.ResponseHeader;
import com.distributedeventing.codec.kafka.values.Array;
import io.netty.buffer.ByteBuf;
import com.distributedeventing.codec.kafka.TestUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class DeleteTopicsResponseTest {

    @Test
    public void testV4_1() {
        String responseHex = "00 00 00 04 00 00 00 00 00 02 09 6d 79 2d 74 6f 70 69 63 00 00 00 00";
        ByteBuf responseBuffer = TestUtils.hexToBinary(responseHex);
        DeleteTopicsResponse response = DeleteTopicsResponse.getInstance((short) 4);
        try {
            response.readFrom(responseBuffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        ResponseHeader header = response.header();
        assertEquals(4, header.correlationId().value());
        assertFalse(header.taggedFields().hasNext());
        assertEquals(0, response.throttleTimeMs().value());
        Array<DeletableTopicResult>.ElementIterator responses = response.responses();
        assertTrue(responses.hasNext());
        DeletableTopicResult topicResult = responses.next();
        assertFalse(responses.hasNext());
        assertEquals("my-topic", topicResult.name().valueAsString());
        assertEquals(0, topicResult.errorCode().value());
        assertFalse(topicResult.taggedFields().hasNext());
        assertFalse(response.taggedFields().hasNext());
    }
}