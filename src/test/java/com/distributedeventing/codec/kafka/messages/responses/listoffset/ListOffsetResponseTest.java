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
package com.distributedeventing.codec.kafka.messages.responses.listoffset;

import com.distributedeventing.codec.kafka.exceptions.ParseException;
import com.distributedeventing.codec.kafka.values.Array;
import io.netty.buffer.ByteBuf;
import com.distributedeventing.codec.kafka.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class ListOffsetResponseTest {

    @Test
    public void testV5_1() {
        String responseHex = "00 00 00 08 00 00 00 00 00 00 00 01 00 0a 63 6f 64 65 63 2d 74 65 73 74 00 00 00 01 " +
                             "00 00 00 00 00 00 ff ff ff ff ff ff ff ff 00 00 00 00 00 00 00 00 00 00 00 00";
        ByteBuf reponseBuffer = TestUtils.hexToBinary(responseHex);
        ListOffsetResponse response = ListOffsetResponse.getInstance((short) 5);
        try {
            response.readFrom(reponseBuffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        assertEquals(8, response.header().correlationId().value());
        assertEquals(0, response.throttleTimeMs().value());
        Array<ListOffsetTopicResponse>.ElementIterator topics = response.topics();
        assertTrue(topics.hasNext());
        ListOffsetTopicResponse topic = topics.next();
        assertFalse(topics.hasNext());
        Assert.assertEquals("codec-test", topic.name().valueAsString());
        Array<ListOffsetPartitionResponse>.ElementIterator partitions = topic.partitions();
        assertTrue(partitions.hasNext());
        ListOffsetPartitionResponse partition = partitions.next();
        assertEquals(0, partition.partitionId().value());
        assertEquals(0, partition.errorCode().value());
        assertEquals(-1, partition.timestamp().value());
        assertEquals(0, partition.offset().value());
        assertEquals(0, partition.leaderEpoch().value());
        assertFalse(partitions.hasNext());
    }
}