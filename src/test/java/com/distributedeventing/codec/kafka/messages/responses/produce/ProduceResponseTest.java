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
package com.distributedeventing.codec.kafka.messages.responses.produce;

import com.distributedeventing.codec.kafka.exceptions.ParseException;
import io.netty.buffer.ByteBuf;
import com.distributedeventing.codec.kafka.TestUtils;
import com.distributedeventing.codec.kafka.values.Array;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class ProduceResponseTest {

    @Test
    public void testV8_1() {
        String responseHex = "00 00 00 05 00 00 00 01 00 0a 63 6f 64 65 63 2d 74 65 73 74 00 00 00 01 00 00 00 00 " +
                             "00 00 00 00 00 00 00 00 00 03 ff ff ff ff ff ff ff ff 00 00 00 00 00 00 00 00 00 00 " +
                             "00 00 ff ff 00 00 00 00";
        ByteBuf responseBuffer = TestUtils.hexToBinary(responseHex);
        ProduceResponse response = ProduceResponse.getInstance((short) 8);
        try {
            response.readFrom(responseBuffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        assertEquals(5, response.header().correlationId().value());
        Array<TopicProduceResponse>.ElementIterator topics = response.responses();
        assertTrue(topics.hasNext());
        TopicProduceResponse topic1 = topics.next();
        assertFalse(topics.hasNext());
        Assert.assertEquals("codec-test", topic1.name().valueAsString());
        Array<PartitionProduceResponse>.ElementIterator partitions = topic1.partitions();
        assertTrue(partitions.hasNext());
        PartitionProduceResponse partition1 = partitions.next();
        assertFalse(partitions.hasNext());
        assertEquals(0, partition1.partitionIndex().value());
        assertEquals(0, partition1.errorCode().value());
        assertEquals(3, partition1.baseOffset().value());
        assertEquals(-1, partition1.logAppendTimeMs().value());
        assertEquals(0, partition1.logStartOffset().value());
        assertFalse(partition1.recordErrors().hasNext());
        assertTrue(partition1.errorMessage().isNull());
        assertEquals(0, response.throttleTimeMs().value());
    }
}