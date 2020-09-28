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
package com.distributedeventing.codec.kafka.messages.requests.fetch;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.distributedeventing.codec.kafka.messages.requests.RequestTestUtils;
import com.distributedeventing.codec.kafka.values.Int32;
import com.distributedeventing.codec.kafka.values.Int64;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FetchPartitionTest {

    @Test
    public void testV9_1() {
        ByteBuf buffer = Unpooled.buffer();
        FetchPartitionV9_1.PARTITION.writeTo(buffer);
        ByteBuf expectedBuffer = FetchPartitionV9_1.getExpectedBuffer();
        Assert.assertEquals(expectedBuffer.readableBytes(), FetchPartitionV9_1.PARTITION.sizeInBytes());
        assertEquals(expectedBuffer, buffer);
    }

    @Test
    public void testV9_2() {
        ByteBuf buffer = Unpooled.buffer();
        FetchPartitionV9_2.PARTITION.writeTo(buffer);
        ByteBuf expectedBuffer = FetchPartitionV9_2.getExpectedBuffer();
        Assert.assertEquals(expectedBuffer.readableBytes(), FetchPartitionV9_2.PARTITION.sizeInBytes());
        assertEquals(expectedBuffer, buffer);
    }

    public static final class FetchPartitionV9_1 {

        public static final Int32 PARTITION_INDEX = new Int32(19);

        public static final Int32 CURRENT_LEADER_EPOCH = new Int32(1500);

        public static final Int64 FETCH_OFFSET = new Int64(2399);

        public static final Int64 LOG_START_OFFSET = new Int64();

        public static final Int32 MAX_BYTES = new Int32(1024);

        public static final FetchPartition PARTITION = FetchPartition.getInstance((short) 9);
        static {
            PARTITION.partitionIndex(PARTITION_INDEX.value());
            PARTITION.currentLeaderEpoch(CURRENT_LEADER_EPOCH.value());
            PARTITION.fetchOffset(FETCH_OFFSET.value());
            PARTITION.logStartOffset(LOG_START_OFFSET.value());
            PARTITION.maxBytes(MAX_BYTES.value());
        }

        public static ByteBuf getExpectedBuffer() {
            return RequestTestUtils.getExpectedBuffer(PARTITION_INDEX,
                                                      CURRENT_LEADER_EPOCH,
                                                      FETCH_OFFSET,
                                                      LOG_START_OFFSET,
                                                      MAX_BYTES);
        }
    }

    public static final class FetchPartitionV9_2 {

        public static final Int32 PARTITION_INDEX = new Int32(0);

        public static final Int32 CURRENT_LEADER_EPOCH = new Int32();

        public static final Int64 FETCH_OFFSET = new Int64();

        public static final Int64 LOG_START_OFFSET = new Int64();

        public static final Int32 MAX_BYTES = new Int32(102400);

        public static final FetchPartition PARTITION = FetchPartition.getInstance((short) 9);
        static {
            PARTITION.partitionIndex(PARTITION_INDEX.value());
            PARTITION.currentLeaderEpoch(CURRENT_LEADER_EPOCH.value());
            PARTITION.fetchOffset(FETCH_OFFSET.value());
            PARTITION.logStartOffset(LOG_START_OFFSET.value());
            PARTITION.maxBytes(MAX_BYTES.value());
        }

        public static ByteBuf getExpectedBuffer() {
            return RequestTestUtils.getExpectedBuffer(PARTITION_INDEX,
                                                      CURRENT_LEADER_EPOCH,
                                                      FETCH_OFFSET,
                                                      LOG_START_OFFSET,
                                                      MAX_BYTES);
        }
    }
}