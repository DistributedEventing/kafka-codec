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

import com.distributedeventing.codec.kafka.TestUtils;
import com.distributedeventing.codec.kafka.messages.requests.RequestTestUtils;
import com.distributedeventing.codec.kafka.values.CompactArray;
import com.distributedeventing.codec.kafka.values.Int32;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CreatableReplicaAssignmentTest {

    @Test
    public void testV5_1() {
        ByteBuf buffer = Unpooled.buffer();
        AssignmentV5_1.ASSIGNMENT.writeTo(buffer);
        ByteBuf expectedBuffer = AssignmentV5_1.getExpectedBuffer();
        Assert.assertEquals(expectedBuffer.readableBytes(), AssignmentV5_1.ASSIGNMENT.sizeInBytes());
        assertEquals(expectedBuffer, buffer);
    }

    public static final class AssignmentV5_1 {

        public static final Int32 PARTITION = new Int32(9);

        public static final Int32 BROKER1 = new Int32(0);

        public static final Int32 BROKER2 = new Int32(5);

        public static final Int32 BROKER3 = new Int32(9);

        public static final CompactArray<Int32> BROKER_IDS = new CompactArray<>(Int32.FACTORY);
        static {
            BROKER_IDS.add(BROKER1);
            BROKER_IDS.add(BROKER2);
            BROKER_IDS.add(BROKER3);
        }

        public static final CreatableReplicaAssignment ASSIGNMENT = CreatableReplicaAssignment.getInstance((short) 5);
        static {
            ASSIGNMENT.partitionIndex(PARTITION.value());
            ASSIGNMENT.brokerIds(BROKER1, BROKER2, BROKER3);
        }

        public static ByteBuf getExpectedBuffer() {
            return RequestTestUtils.getExpectedBuffer(AssignmentV5_1.PARTITION,
                                                      AssignmentV5_1.BROKER_IDS,
                                                      TestUtils.TAGGED_FIELDS);
        }
    }
}