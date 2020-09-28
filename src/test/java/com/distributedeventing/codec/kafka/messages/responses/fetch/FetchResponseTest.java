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
package com.distributedeventing.codec.kafka.messages.responses.fetch;

import com.distributedeventing.codec.kafka.exceptions.ParseException;
import com.distributedeventing.codec.kafka.headers.ResponseHeader;
import com.distributedeventing.codec.kafka.values.Array;
import com.distributedeventing.codec.kafka.values.NvlBytes;
import com.distributedeventing.codec.kafka.values.records.RecordBatch;
import com.distributedeventing.codec.kafka.values.records.RecordBatchArray;
import io.netty.buffer.ByteBuf;
import com.distributedeventing.codec.kafka.TestUtils;
import com.distributedeventing.codec.kafka.values.records.Record;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class FetchResponseTest {

    @Test
    public void testV11_1() {
        String responseHex = "00 00 00 0a 00 00 00 00 00 00 55 96 28 3f 00 00 00 01 00 0a 63 6f 64 65 63 2d " +
                             "74 65 73 74 00 00 00 01 00 00 00 00 00 00 00 00 00 00 00 00 00 02 00 00 00 00 " +
                             "00 00 00 02 00 00 00 00 00 00 00 00 ff ff ff ff ff ff ff ff 00 00 00 b6 00 00 " +
                             "00 00 00 00 00 00 00 00 00 4f 00 00 00 00 02 d6 95 6b 31 00 00 00 00 00 00 00 " +
                             "00 01 74 80 de 8e 90 00 00 01 74 80 de 8e 90 ff ff ff ff ff ff ff ff ff ff ff " +
                             "ff ff ff 00 00 00 01 3a 00 00 00 01 2e 54 65 73 74 20 6d 73 67 20 31 20 66 72 " +
                             "6f 6d 20 63 6f 6e 73 6f 6c 65 00 00 00 00 00 00 00 00 01 00 00 00 4f 00 00 00 " +
                             "00 02 64 ed ab 85 00 00 00 00 00 00 00 00 01 74 80 de 9f 41 00 00 01 74 80 de " +
                             "9f 41 ff ff ff ff ff ff ff ff ff ff ff ff ff ff 00 00 00 01 3a 00 00 00 01 2e " +
                             "54 65 73 74 20 6d 73 67 20 32 20 66 72 6f 6d 20 63 6f 6e 73 6f 6c 65 00";
        ByteBuf reponseBuffer = TestUtils.hexToBinary(responseHex);
        FetchResponse response = FetchResponse.getInstance((short) 11);
        try {
            response.readFrom(reponseBuffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        ResponseHeader header = response.header();
        assertEquals(10, header.correlationId().value());
        assertEquals(0, response.throttleTimeMs().value());
        assertEquals(0, response.errorCode().value());
        assertEquals(1435904063, response.sessionId().value());
        Array<FetchableTopicResponse>.ElementIterator topics = response.topics();
        assertTrue(topics.hasNext());
        FetchableTopicResponse topic = topics.next();
        assertFalse(topics.hasNext());
        Assert.assertEquals("codec-test", topic.topic().valueAsString());
        Array<FetchablePartitionResponse>.ElementIterator partitions = topic.partitions();
        assertTrue(partitions.hasNext());
        FetchablePartitionResponse partition = partitions.next();
        assertFalse(partitions.hasNext());
        assertEquals(0, partition.partitionIndex().value());
        assertEquals(0, partition.errorCode().value());
        assertEquals(2, partition.highWatermark().value());
        assertEquals(2, partition.lastStableOffset().value());
        assertEquals(0, partition.logStartOffset().value());
        Array<AbortedTransaction>.ElementIterator abortedTransactions = partition.abortedTransactions();
        assertFalse(abortedTransactions.hasNext());
        assertEquals(-1, partition.preferredReadReplica().value());
        RecordBatchArray.ElementIterator records = partition.records();
        assertTrue(records.hasNext());
        RecordBatch batch1 = records.next();
        assertEquals(0, batch1.baseOffset().value());
        assertEquals(79, batch1.length().value());
        assertEquals(0, batch1.partitionLeaderEpoch().value());
        assertEquals(2, batch1.magic().value());
        assertEquals(0, batch1.attributes().value());
        assertEquals(0, batch1.lastOffsetDelta().value());
        assertEquals(1599889903248L, batch1.firstTimestamp().value());
        assertEquals(1599889903248L, batch1.maxTimestamp().value());
        assertEquals(-1, batch1.producerId().value());
        assertEquals(-1, batch1.producerEpoch().value());
        assertEquals(-1, batch1.baseSequence().value());
        Array<Record>.ElementIterator records1 = batch1.records();
        assertTrue(records1.hasNext());
        Record record1 = records1.next();
        assertEquals(0, record1.attributes().value());
        assertEquals(0, record1.timestampDelta().value());
        assertEquals(0, record1.offsetDelta().value());
        assertTrue(record1.key().isNull());
        assertEqual("Test msg 1 from console", record1.value());
        assertFalse(record1.headers().hasNext());
        assertFalse(records1.hasNext());
        assertTrue(records.hasNext());
        RecordBatch batch2 = records.next();
        assertEquals(1, batch2.baseOffset().value());
        assertEquals(79, batch2.length().value());
        assertEquals(0, batch2.partitionLeaderEpoch().value());
        assertEquals(2, batch2.magic().value());
        assertEquals(0, batch2.attributes().value());
        assertEquals(0, batch2.lastOffsetDelta().value());
        assertEquals(1599889907521L, batch2.firstTimestamp().value());
        assertEquals(1599889907521L, batch2.maxTimestamp().value());
        assertEquals(-1, batch2.producerId().value());
        assertEquals(-1, batch2.producerEpoch().value());
        assertEquals(-1, batch2.baseSequence().value());
        Array<Record>.ElementIterator records2 = batch2.records();
        assertTrue(records2.hasNext());
        Record record2 = records2.next();
        assertEquals(0, record2.attributes().value());
        assertEquals(0, record2.timestampDelta().value());
        assertEquals(0, record2.offsetDelta().value());
        assertTrue(record2.key().isNull());
        assertEqual("Test msg 2 from console", record2.value());
        assertFalse(record2.headers().hasNext());
        assertFalse(records2.hasNext());
        assertFalse(records.hasNext());
    }

    private void assertEqual(String s, NvlBytes value) {
        assertEquals(s, new String(value.value(), 0, value.length()));
    }
}