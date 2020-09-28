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

import com.distributedeventing.codec.kafka.ByteUtils;
import com.distributedeventing.codec.kafka.TestUtils;
import com.distributedeventing.codec.kafka.values.CompactArray;
import com.distributedeventing.codec.kafka.values.CompactString;
import com.distributedeventing.codec.kafka.values.Int32;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.distributedeventing.codec.kafka.messages.requests.RequestTestUtils;
import com.distributedeventing.codec.kafka.values.Int16;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CreateableTopicTest {

    @Test
    public void testV5_1() {
        ByteBuf buffer = Unpooled.buffer();
        CreateableTopicV5_1.TOPIC.writeTo(buffer);
        ByteBuf expectedBuffer = CreateableTopicV5_1.getExpectedBuffer();
        Assert.assertEquals(expectedBuffer.readableBytes(), CreateableTopicV5_1.TOPIC.sizeInBytes());
        assertTrue(expectedBuffer.equals(buffer));
    }

    @Test
    public void testV5_2() {
        ByteBuf buffer = Unpooled.buffer();
        CreateableTopicV5_2.TOPIC.writeTo(buffer);
        ByteBuf expectedBuffer = CreateableTopicV5_2.getExpectedBuffer();
        Assert.assertEquals(expectedBuffer.readableBytes(), CreateableTopicV5_2.TOPIC.sizeInBytes());
        assertTrue(expectedBuffer.equals(buffer));
    }

    public static final class CreateableTopicV5_1 {

        public static final byte[] NAME_BYTES = "test1".getBytes(ByteUtils.CHARSET_UTF8);

        public static final CompactString NAME = new CompactString();
        static {
            NAME.value(NAME_BYTES);
        }

        public static final Int32 NUM_PARTITIONS = new Int32(1);

        public static final Int16 REPLICATION_FACTOR = new Int16((short) 3);

        public static final CompactArray<CreatableReplicaAssignment> ASSIGNMENTS =
                new CompactArray<>(new CreatableReplicaAssignment.CreatableReplicaAssignmentFactory((short) 5));
        static {
            ASSIGNMENTS.add(CreatableReplicaAssignmentTest.AssignmentV5_1.ASSIGNMENT);
        }

        public static final CompactArray<CreateableTopicConfig> CONFIGS =
                new CompactArray<>(new CreateableTopicConfig.CreateableTopicConfigFactory((short) 5));
        static {
            CONFIGS.add(CreateableTopicConfigTest.ConfigV5_1.CONFIG);
        }

        public static final CreateableTopic TOPIC = CreateableTopic.getInstance((short) 5);
        static {
            TOPIC.name(NAME_BYTES);
            TOPIC.numPartitions(NUM_PARTITIONS.value());
            TOPIC.replicationFactor(REPLICATION_FACTOR.value());
            CreatableReplicaAssignment assignment = TOPIC.createAssignment();
            assignment.partitionIndex(CreatableReplicaAssignmentTest.AssignmentV5_1.PARTITION.value());
            assignment.brokerIds(CreatableReplicaAssignmentTest.AssignmentV5_1.BROKER1,
                                 CreatableReplicaAssignmentTest.AssignmentV5_1.BROKER2,
                                 CreatableReplicaAssignmentTest.AssignmentV5_1.BROKER3);
            CreateableTopicConfig config = TOPIC.createConfig();
            config.name(CreateableTopicConfigTest.ConfigV5_1.NAME_BYTES);
            config.value(CreateableTopicConfigTest.ConfigV5_1.VALUE_BYTES);
        }

        public static ByteBuf getExpectedBuffer() {
            return RequestTestUtils.getExpectedBuffer(NAME,
                                                      NUM_PARTITIONS,
                                                      REPLICATION_FACTOR,
                                                      ASSIGNMENTS,
                                                      CONFIGS,
                                                      TestUtils.TAGGED_FIELDS);
        }
    }

    public static final class CreateableTopicV5_2 {

        public static final byte[] NAME_BYTES =
                "some-very-long-topic-name.that.has.been.made.up.for.test".getBytes(ByteUtils.CHARSET_UTF8);

        public static final CompactString NAME = new CompactString();
        static {
            NAME.value(NAME_BYTES);
        }

        public static final Int32 NUM_PARTITIONS = new Int32(-1); // default partitions

        public static final Int16 REPLICATION_FACTOR = new Int16((short) -1); // default replication factor

        public static final CompactArray<CreatableReplicaAssignment> ASSIGNMENTS = // auto assignment
                new CompactArray<>(new CreatableReplicaAssignment.CreatableReplicaAssignmentFactory((short) 5));

        public static final CompactArray<CreateableTopicConfig> CONFIGS = // no config set
                new CompactArray<>(new CreateableTopicConfig.CreateableTopicConfigFactory((short) 5));

        public static final CreateableTopic TOPIC = CreateableTopic.getInstance((short) 5);
        static {
            TOPIC.name(NAME_BYTES, 0, NAME_BYTES.length);
            TOPIC.numPartitions(NUM_PARTITIONS.value());
            TOPIC.replicationFactor(REPLICATION_FACTOR.value());
        }

        public static ByteBuf getExpectedBuffer() {
            return RequestTestUtils.getExpectedBuffer(NAME,
                                                      NUM_PARTITIONS,
                                                      REPLICATION_FACTOR,
                                                      ASSIGNMENTS,
                                                      CONFIGS,
                                                      TestUtils.TAGGED_FIELDS);
        }
    }
}