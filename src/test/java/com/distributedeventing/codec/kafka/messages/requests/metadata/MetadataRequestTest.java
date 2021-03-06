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
package com.distributedeventing.codec.kafka.messages.requests.metadata;

import com.distributedeventing.codec.kafka.headers.RequestHeader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.distributedeventing.codec.kafka.ByteUtils;
import com.distributedeventing.codec.kafka.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetadataRequestTest {

    @Test
    public void testV9_1() {
        ByteBuf buffer = Unpooled.buffer();
        MetadataRequestV9_1.REQUEST.writeTo(buffer);
        ByteBuf expectedBuffer = MetadataRequestV9_1.getExpectedBuffer();
        Assert.assertEquals(expectedBuffer.readableBytes(), MetadataRequestV9_1.REQUEST.sizeInBytes());
        assertTrue(expectedBuffer.equals(buffer));
    }

    public static final class MetadataRequestV9_1 {

        // Actual metadata request sent by Kafka console consumer
        public static final MetadataRequest REQUEST = MetadataRequest.getInstance(9);
        static {
            MetadataRequestTopic topic = REQUEST.createTopic();
            topic.name("codec-test".getBytes(ByteUtils.CHARSET_UTF8));
            REQUEST.allowAutoTopicCreation(true);
            RequestHeader header = REQUEST.header();
            header.clientId("consumer-console-consumer-59750-1".getBytes(ByteUtils.CHARSET_UTF8));
            header.correlationId(2);
        }

        public static ByteBuf getExpectedBuffer() {
            // Actual hex dump of a metadata request sent by Kafka console consumer
            String msgHex = "00 03 00 09 00 00 00 02 00 21 63 6f 6e 73 75 6d 65 72 2d 63 6f 6e 73 6f 6c 65 2d 63 " +
                            "6f 6e 73 75 6d 65 72 2d 35 39 37 35 30 2d 31 00 02 0b 63 6f 64 65 63 2d 74 65 73 74 " +
                            "00 01 00 00 00";
            return TestUtils.hexToBinary(msgHex);
        }
    }
}