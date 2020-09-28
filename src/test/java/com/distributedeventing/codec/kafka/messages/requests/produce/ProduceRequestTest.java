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
package com.distributedeventing.codec.kafka.messages.requests.produce;

import com.distributedeventing.codec.kafka.ByteUtils;
import com.distributedeventing.codec.kafka.headers.RequestHeader;
import com.distributedeventing.codec.kafka.values.records.Record;
import com.distributedeventing.codec.kafka.values.records.RecordBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.distributedeventing.codec.kafka.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProduceRequestTest {

    @Test
    public void testV8_1() {
        ByteBuf buffer = Unpooled.buffer();
        ProduceRequestV8_1.REQUEST.writeTo(buffer);
        ByteBuf expectedBuffer = ProduceRequestV8_1.getExpectedBuffer();
        Assert.assertEquals(expectedBuffer.readableBytes(), ProduceRequestV8_1.REQUEST.sizeInBytes());

        /**
         * Not every byte in the two buffers will match. Specifically, the bytes
         * that correspond to timestamps and CRC will not match.
         */

        int lengthTillMagic = 73;
        ByteBuf expectedBufferP1 = expectedBuffer.slice(0, lengthTillMagic);
        ByteBuf bufferP1 = buffer.slice(0, lengthTillMagic);
        assertTrue(expectedBufferP1.equals(bufferP1));

        int attributesOffset = lengthTillMagic + 4; // length of CRC = 4
        int lenOfAttrsAndLastOffsetDelta = 6;
        ByteBuf expectedBufferP2 = expectedBuffer.slice(attributesOffset, lenOfAttrsAndLastOffsetDelta);
        ByteBuf bufferP2 = buffer.slice(attributesOffset, lenOfAttrsAndLastOffsetDelta);
        assertTrue(expectedBufferP2.equals(bufferP2));

        int producerIdOffset = attributesOffset + lenOfAttrsAndLastOffsetDelta + 16; // 16 = length of first and max timestamp
        ByteBuf expectedBufferP3 = expectedBuffer.slice(producerIdOffset, expectedBuffer.readableBytes() - producerIdOffset);
        ByteBuf bufferP3 = buffer.slice(producerIdOffset, buffer.readableBytes() - producerIdOffset);
        assertTrue(expectedBufferP3.equals(bufferP3));
    }

    public static final class ProduceRequestV8_1 {

        // Actual produce request sent to Kafka
        public static final ProduceRequest REQUEST = ProduceRequest.getInstance(8);
        static {
            REQUEST.transactionalId(null);
            REQUEST.acks(ProduceRequest.Acks.LEADER_ONLY);
            REQUEST.timeoutMs(1500);
            TopicProduceData topic = REQUEST.createTopic();
            topic.name("test".getBytes(ByteUtils.CHARSET_UTF8));
            PartitionProduceData partition = topic.createPartition();
            partition.partitionIndex(1);
            RecordBatch recordBatch = partition.createRecordBatch();
            Record record = recordBatch.createRecord();
            record.value("Test message one".getBytes(ByteUtils.CHARSET_UTF8));
            RequestHeader header = REQUEST.header();
            header.clientId("console-producer".getBytes(ByteUtils.CHARSET_UTF8));
            header.correlationId(4);
        }

        public static ByteBuf getExpectedBuffer() {
            // Actual hex dump of a produce request sent by Kafka console producer
            String msgHex = "00 00 00 08 00 00 00 04 00 10 63 6f 6e 73 6f 6c 65 2d 70 72 6f 64 75 63 65 72 " +
                            "ff ff 00 01 00 00 05 dc 00 00 00 01 00 04 74 65 73 74 00 00 00 01 00 00 00 01 " +
                            "00 00 00 54 00 00 00 00 00 00 00 00 00 00 00 48 ff ff ff ff 02 66 51 28 26 00 " +
                            "00 00 00 00 00 00 00 01 74 7b 9e db 66 00 00 01 74 7b 9e db 66 ff ff ff ff ff " +
                            "ff ff ff ff ff ff ff ff ff 00 00 00 01 2c 00 00 00 01 20 54 65 73 74 20 6d 65 " +
                            "73 73 61 67 65 20 6f 6e 65 00";
            return TestUtils.hexToBinary(msgHex);
        }
    }
}