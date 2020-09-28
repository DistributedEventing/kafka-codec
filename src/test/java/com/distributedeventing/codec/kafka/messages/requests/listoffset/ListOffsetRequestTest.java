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
package com.distributedeventing.codec.kafka.messages.requests.listoffset;

import com.distributedeventing.codec.kafka.ByteUtils;
import com.distributedeventing.codec.kafka.headers.RequestHeader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.distributedeventing.codec.kafka.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ListOffsetRequestTest {

    @Test
    public void testV5_1() {
        ByteBuf buffer = Unpooled.buffer();
        ListOffsetRequestV5_1.REQUEST.writeTo(buffer);
        ByteBuf expectedBuffer = ListOffsetRequestV5_1.getExpectedBuffer();
        Assert.assertEquals(expectedBuffer.readableBytes(), ListOffsetRequestV5_1.REQUEST.sizeInBytes());
        // Match everything except for the timestamp at the end
        ByteBuf expectedBuffer1 = expectedBuffer.slice(0, expectedBuffer.readableBytes() - 8);
        ByteBuf buffer1 = buffer.slice(0, buffer.readableBytes() - 8);
        assertTrue(expectedBuffer1.equals(buffer1));
    }

    public static final class ListOffsetRequestV5_1 {

        // Actual list offset request sent by Kafka console consumer
        public static final ListOffsetRequest REQUEST = ListOffsetRequest.getInstance(5);
        static {
            ListOffsetTopic topic = REQUEST.createTopic();
            topic.name("codec-test".getBytes(ByteUtils.CHARSET_UTF8));
            ListOffsetPartition partition = topic.createPartition();
            RequestHeader header = REQUEST.header();
            header.clientId("consumer-console-consumer-59750-1".getBytes(ByteUtils.CHARSET_UTF8));
            header.correlationId(8);
        }


        public static ByteBuf getExpectedBuffer() {
            // Actual hex dump of a list offset request sent by Kafka console consumer
            String msgHex = "00 02 00 05 00 00 00 08 00 21 63 6f 6e 73 75 6d 65 72 2d 63 6f 6e 73 6f 6c 65 2d 63 " +
                            "6f 6e 73 75 6d 65 72 2d 35 39 37 35 30 2d 31 ff ff ff ff 00 00 00 00 01 00 0a 63 6f " +
                            "64 65 63 2d 74 65 73 74 00 00 00 01 00 00 00 00 00 00 00 00 ff ff ff ff ff ff ff fe";
            return TestUtils.hexToBinary(msgHex);
        }
    }
}