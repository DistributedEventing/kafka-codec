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
package com.distributedeventing.codec.kafka.messages.responses.metadata;

import com.distributedeventing.codec.kafka.exceptions.ParseException;
import com.distributedeventing.codec.kafka.headers.ResponseHeader;
import com.distributedeventing.codec.kafka.values.Array;
import io.netty.buffer.ByteBuf;
import com.distributedeventing.codec.kafka.TestUtils;
import com.distributedeventing.codec.kafka.values.Int32;
import org.junit.Test;

import static org.junit.Assert.*;

public class MetadataResponseTest {

    @Test
    public void testV9_1() {
        String responseHex = "00 00 00 02 00 00 00 00 00 02 00 00 00 00 0e 31 39 32 2e 31 36 38 2e 35 30 2e 39 31 " +
                             "00 00 23 84 00 00 17 34 50 59 7a 6d 52 35 65 52 4e 32 6d 65 75 59 6c 39 73 6d 68 42 " +
                             "67 00 00 00 00 02 00 00 0b 63 6f 64 65 63 2d 74 65 73 74 00 02 00 00 00 00 00 00 00 " +
                             "00 00 00 00 00 00 00 02 00 00 00 00 02 00 00 00 00 01 00 80 00 00 00 00 80 00 00 00 00";
        ByteBuf responseBuffer = TestUtils.hexToBinary(responseHex);
        MetadataResponse response = MetadataResponse.getInstance((short) 9);
        try {
            response.readFrom(responseBuffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        ResponseHeader header = response.header();
        assertEquals(2, header.correlationId().value());
        assertFalse(header.taggedFields().hasNext());
        assertEquals(0, response.throttleTimeMs().value());
        Array<MetadataResponseBroker>.ElementIterator brokers = response.brokers();
        assertTrue(brokers.hasNext());
        MetadataResponseBroker broker1 = brokers.next();
        assertFalse(brokers.hasNext());
        assertEquals(0, broker1.nodeId().value());
        assertEquals(13, broker1.host().length()); // not putting the actual IP address here
        assertEquals(9092, broker1.port().value());
        assertTrue(broker1.rack().isNull());
        assertFalse(broker1.taggedFields().hasNext());
        assertEquals("4PYzmR5eRN2meuYl9smhBg", response.clusterId().valueAsString());
        assertEquals(0, response.controlledId().value());
        Array<MetadataResponseTopic>.ElementIterator topics = response.topics();
        assertTrue(topics.hasNext());
        MetadataResponseTopic topic1 = topics.next();
        assertFalse(topics.hasNext());
        assertEquals(0, topic1.errorCode().value());
        assertEquals("codec-test", topic1.name().valueAsString());
        assertFalse(topic1.isInternal().value());
        Array<MetadataResponsePartition>.ElementIterator partitions = topic1.partitions();
        assertTrue(partitions.hasNext());
        MetadataResponsePartition partition1 = partitions.next();
        assertFalse(partitions.hasNext());
        assertEquals(0, partition1.errorcode().value());
        assertEquals(0, partition1.partitionIndex().value());
        assertEquals(0, partition1.leaderId().value());
        assertEquals(0, partition1.leaderEpoch().value());
        Array<Int32>.ElementIterator replicaNodes = partition1.replicaNodes();
        assertTrue(replicaNodes.hasNext());
        Int32 replica1 = replicaNodes.next();
        assertFalse(replicaNodes.hasNext());
        assertEquals(0, replica1.value());
        Array<Int32>.ElementIterator isrNodes = partition1.isrNodes();
        assertTrue(isrNodes.hasNext());
        Int32 isrNode1 = isrNodes.next();
        assertFalse(isrNodes.hasNext());
        assertEquals(0, isrNode1.value());
        Array<Int32>.ElementIterator offlineReplicas = partition1.offlineReplicas();
        assertFalse(offlineReplicas.hasNext());
        assertFalse(partition1.taggedFields().hasNext());
        assertEquals(-2147483648, topic1.topicAuthorizedOperations().value());
        assertFalse(topic1.taggedFields().hasNext());
        assertEquals(-2147483648, response.clusterAuthorizedOperations().value());
        assertFalse(response.taggedFields().hasNext());
    }
}