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
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ApiVersionsRequestTest {

    @Test
    public void testV3_1() {
        ByteBuf buffer = Unpooled.buffer();
        ApiVersionsRequestV3_1.REQUEST.writeTo(buffer);
        ByteBuf expectedBuffer = ApiVersionsRequestV3_1.getExpectedBuffer();
        Assert.assertEquals(expectedBuffer.readableBytes(), ApiVersionsRequestV3_1.REQUEST.sizeInBytes());
        assertTrue(expectedBuffer.equals(buffer));
    }

    public static final class ApiVersionsRequestV3_1 {

        // Actual request sent to Kafka broker
        public static final ApiVersionsRequest REQUEST = ApiVersionsRequest.getInstance(3);
        static {
            REQUEST.clientSoftwareName("apache-kafka-java".getBytes(ByteUtils.CHARSET_UTF8));
            REQUEST.clientSoftwareVersion("2.5.0".getBytes(ByteUtils.CHARSET_UTF8));
            RequestHeader header = REQUEST.header();
            header.clientId("adminclient-1".getBytes(ByteUtils.CHARSET_UTF8));
            header.correlationId(2);
        }

        public static ByteBuf getExpectedBuffer() {
            // Actual hex dump of an api versions request sent by Kafka admin client
            String msgHex = "00 12 00 03 00 00 00 02 00 0d 61 64 6d 69 6e 63 6c 69 65 6e 74 2d 31 00 12 61 70 " +
                            "61 63 68 65 2d 6b 61 66 6b 61 2d 6a 61 76 61 06 32 2e 35 2e 30 00";
            return TestUtils.hexToBinary(msgHex);
        }
    }
}