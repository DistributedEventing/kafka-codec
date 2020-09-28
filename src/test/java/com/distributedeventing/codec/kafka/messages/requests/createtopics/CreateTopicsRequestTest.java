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
package com.distributedeventing.codec.kafka.messages.requests.createtopics;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.distributedeventing.codec.kafka.ByteUtils;
import com.distributedeventing.codec.kafka.TestUtils;
import com.distributedeventing.codec.kafka.headers.RequestHeader;
import com.distributedeventing.codec.kafka.messages.requests.RequestTestUtils;
import com.distributedeventing.codec.kafka.values.Boolean;
import com.distributedeventing.codec.kafka.values.CompactArray;
import com.distributedeventing.codec.kafka.values.Int32;
import org.junit.Test;

import static com.distributedeventing.codec.kafka.TestUtils.TAGGED_FIELDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CreateTopicsRequestTest {

    @Test
    public void testV5_1() {
        ByteBuf buffer = Unpooled.buffer();
        CreateTopicsRequestV5_1.REQUEST.writeTo(buffer);
        ByteBuf expectedBuffer = CreateTopicsRequestV5_1.getExpectedBuffer();
        assertEquals(expectedBuffer.readableBytes(), CreateTopicsRequestV5_1.REQUEST.sizeInBytes());
        assertTrue(expectedBuffer.equals(buffer));
    }

    @Test
    public void testV5_2() {
        ByteBuf buffer = Unpooled.buffer();
        CreateTopicsRequestV5_2.REQUEST.writeTo(buffer);
        ByteBuf expectedBuffer = CreateTopicsRequestV5_2.getExpectedBuffer();
        assertEquals(expectedBuffer.readableBytes(), CreateTopicsRequestV5_2.REQUEST.sizeInBytes());
        assertTrue(expectedBuffer.equals(buffer));
    }

    @Test
    public void testV5_3() {
        ByteBuf buffer = Unpooled.buffer();
        CreateTopicsRequestV5_3.REQUEST.writeTo(buffer);
        ByteBuf expectedBuffer = CreateTopicsRequestV5_3.getExpectedBuffer();
        assertEquals(expectedBuffer.readableBytes(), CreateTopicsRequestV5_3.REQUEST.sizeInBytes());
        assertTrue(expectedBuffer.equals(buffer));
    }

    @Test
    public void testV5_4() {
        ByteBuf buffer = Unpooled.buffer();
        CreateTopicsRequestV5_4.REQUEST.writeTo(buffer);
        ByteBuf expectedBuffer = CreateTopicsRequestV5_4.getExpectedBuffer();
        assertEquals(expectedBuffer.readableBytes(), CreateTopicsRequestV5_4.REQUEST.sizeInBytes());
        assertTrue(expectedBuffer.equals(buffer));
    }

    public static final class CreateTopicsRequestV5_1 {

        public static final CompactArray<CreateableTopic> TOPICS =
                new CompactArray<>(new CreateableTopic.CreateableTopicFactory((short) 5));
        static {
            TOPICS.add(CreateableTopicTest.CreateableTopicV5_1.TOPIC);
        }

        public static final Int32 TIMEOUT_MS = new Int32(1500);

        public static final Boolean VALIDATE_ONLY = new Boolean(false);

        public static final CreateTopicsRequest REQUEST = CreateTopicsRequest.getInstance(5);
        public static final RequestHeader HEADER = REQUEST.header();
        static {
            CreateableTopic topic = REQUEST.createTopic();
            topic.name(CreateableTopicTest.CreateableTopicV5_1.NAME_BYTES);
            topic.numPartitions(CreateableTopicTest.CreateableTopicV5_1.NUM_PARTITIONS.value());
            topic.replicationFactor(CreateableTopicTest.CreateableTopicV5_1.REPLICATION_FACTOR.value());
            CreatableReplicaAssignment assignment = topic.createAssignment();
            assignment.partitionIndex(CreatableReplicaAssignmentTest.AssignmentV5_1.PARTITION.value());
            assignment.brokerIds(CreatableReplicaAssignmentTest.AssignmentV5_1.BROKER1,
                                 CreatableReplicaAssignmentTest.AssignmentV5_1.BROKER2,
                                 CreatableReplicaAssignmentTest.AssignmentV5_1.BROKER3);
            CreateableTopicConfig config = topic.createConfig();
            config.name(CreateableTopicConfigTest.ConfigV5_1.NAME_BYTES);
            config.value(CreateableTopicConfigTest.ConfigV5_1.VALUE_BYTES);
            REQUEST.timeoutMs(TIMEOUT_MS.value());
            REQUEST.validateOnly(VALIDATE_ONLY.value());
            HEADER.clientId("kafka-codec-test".getBytes(ByteUtils.CHARSET_UTF8));
            HEADER.correlationId(1);
        }

        public static ByteBuf getExpectedBuffer() {
            ByteBuf buffer = Unpooled.buffer();
            HEADER.writeTo(buffer);
            return RequestTestUtils.getExpectedBuffer(buffer,
                                                      TOPICS,
                                                      TIMEOUT_MS,
                                                      VALIDATE_ONLY,
                                                      TAGGED_FIELDS);
        }
    }

    public static final class CreateTopicsRequestV5_2 {

        public static final CompactArray<CreateableTopic> TOPICS =
                new CompactArray<>(new CreateableTopic.CreateableTopicFactory((short) 5));
        static {
            TOPICS.add(CreateableTopicTest.CreateableTopicV5_2.TOPIC);
        }

        public static final Int32 TIMEOUT_MS = new Int32(1500);

        public static final Boolean VALIDATE_ONLY = new Boolean(false);

        public static final CreateTopicsRequest REQUEST = CreateTopicsRequest.getInstance(5);
        public static final RequestHeader HEADER = REQUEST.header();
        static {
            CreateableTopic topic = REQUEST.createTopic();
            topic.name(CreateableTopicTest.CreateableTopicV5_2.NAME_BYTES);
            topic.numPartitions(CreateableTopicTest.CreateableTopicV5_2.NUM_PARTITIONS.value());
            topic.replicationFactor(CreateableTopicTest.CreateableTopicV5_2.REPLICATION_FACTOR.value());
            REQUEST.timeoutMs(TIMEOUT_MS.value());
            REQUEST.validateOnly(VALIDATE_ONLY.value());
            HEADER.clientId("kafka-codec-test".getBytes(ByteUtils.CHARSET_UTF8));
            HEADER.correlationId(2);
        }

        public static ByteBuf getExpectedBuffer() {
            ByteBuf buffer = Unpooled.buffer();
            HEADER.writeTo(buffer);
            return RequestTestUtils.getExpectedBuffer(buffer,
                                                      TOPICS,
                                                      TIMEOUT_MS,
                                                      VALIDATE_ONLY,
                                                      TAGGED_FIELDS);
        }
    }

    public static final class CreateTopicsRequestV5_3 {

        // Actual request sent to Kafka broker
        public static final CreateTopicsRequest REQUEST = CreateTopicsRequest.getInstance(5);
        static {
            CreateableTopic topic = REQUEST.createTopic();
            topic.name("my-topic".getBytes(ByteUtils.CHARSET_UTF8));
            topic.numPartitions(1);
            topic.replicationFactor((short) 1);
            CreateableTopicConfig config1 = topic.createConfig();
            config1.name("max.message.bytes".getBytes(ByteUtils.CHARSET_UTF8));
            config1.value("64000".getBytes(ByteUtils.CHARSET_UTF8));
            CreateableTopicConfig config2 = topic.createConfig();
            config2.name("flush.messages".getBytes(ByteUtils.CHARSET_UTF8));
            config2.value("1".getBytes(ByteUtils.CHARSET_UTF8));
            REQUEST.timeoutMs(30000);
            REQUEST.validateOnly(false);
            RequestHeader header = REQUEST.header();
            header.clientId("adminclient-1".getBytes(ByteUtils.CHARSET_UTF8));
            header.correlationId(4);
        }

        public static ByteBuf getExpectedBuffer() {
            // Actual hex dump of a create topic request sent by Kafka admin client
            String msgHex = "00 13 00 05 00 00 00 04 00 0d 61 64 6d 69 6e 63 6c 69 65 6e 74 2d 31 00 " +
                            "02 09 6d 79 2d 74 6f 70 69 63 00 00 00 01 00 01 01 03 12 6d 61 78 2e 6d 65 73 73 61 " +
                            "67 65 2e 62 79 74 65 73 06 36 34 30 30 30 00 0f 66 6c 75 73 68 2e 6d 65 73 73 61 67 " +
                            "65 73 02 31 00 00 00 00 75 30 00 00";
            return TestUtils.hexToBinary(msgHex);
        }
    }

    public static final class CreateTopicsRequestV5_4 {

        // Actual request sent to Kafka broker
        public static final CreateTopicsRequest REQUEST = CreateTopicsRequest.getInstance(5);
        static {
            CreateableTopic topic = REQUEST.createTopic();
            topic.name("test-topic-2".getBytes(ByteUtils.CHARSET_UTF8));
            topic.numPartitions(20);
            topic.replicationFactor((short) 3);
            REQUEST.timeoutMs(30000);
            REQUEST.validateOnly(false);
            RequestHeader header = REQUEST.header();
            header.clientId("adminclient-1".getBytes(ByteUtils.CHARSET_UTF8));
            header.correlationId(4);
        }

        public static ByteBuf getExpectedBuffer() {
            // Actual hex dump of a create topic request sent by Kafka admin client
            String msgHex = "00 13 00 05 00 00 00 04 00 0d 61 64 6d 69 6e 63 6c 69 65 6e 74 2d 31 00 02 0d 74 65 " +
                            "73 74 2d 74 6f 70 69 63 2d 32 00 00 00 14 00 03 01 01 00 00 00 75 30 00 00";
            return TestUtils.hexToBinary(msgHex);
        }
    }
}