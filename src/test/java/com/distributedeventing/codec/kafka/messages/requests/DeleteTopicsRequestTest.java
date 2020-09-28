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

import com.distributedeventing.codec.kafka.ByteUtils;
import com.distributedeventing.codec.kafka.TestUtils;
import com.distributedeventing.codec.kafka.headers.RequestHeader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.distributedeventing.codec.kafka.values.CompactString;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class DeleteTopicsRequestTest {

    @Test
    public void testV4_1() {
        ByteBuf buffer = Unpooled.buffer();
        DeleteTopicsRequestV4_1.REQUEST.writeTo(buffer);
        ByteBuf expectedBuffer = DeleteTopicsRequestV4_1.getExpectedBuffer();
        Assert.assertEquals(expectedBuffer.readableBytes(), DeleteTopicsRequestV4_1.REQUEST.sizeInBytes());
        assertTrue(expectedBuffer.equals(buffer));
    }

    public static final class DeleteTopicsRequestV4_1 {

        // Actual request sent to kafka broker
        public static final DeleteTopicsRequest REQUEST = DeleteTopicsRequest.getInstance(4);
        static {
            CompactString topicName = new CompactString();
            topicName.value("test".getBytes(ByteUtils.CHARSET_UTF8));
            REQUEST.topicNames(topicName);
            REQUEST.timeoutMs(30000);
            RequestHeader header = REQUEST.header();
            header.clientId("adminclient-1".getBytes(ByteUtils.CHARSET_UTF8));
            header.correlationId(4);
        }

        public static ByteBuf getExpectedBuffer() {
            // Actual hex dump of a delete topic request sent by Kafka admin client
            String msgHex =  "00 14 00 04 00 00 00 04 00 0d 61 64 6d 69 6e 63 6c 69 65 6e 74 2d 31 00 02 05 74 " +
                             "65 73 74 00 00 75 30 00";
            return TestUtils.hexToBinary(msgHex);
        }
    }
}