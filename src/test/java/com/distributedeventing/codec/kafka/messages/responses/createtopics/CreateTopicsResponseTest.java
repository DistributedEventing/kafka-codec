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
package com.distributedeventing.codec.kafka.messages.responses.createtopics;

import com.distributedeventing.codec.kafka.exceptions.ParseException;
import com.distributedeventing.codec.kafka.values.Array;
import io.netty.buffer.ByteBuf;
import com.distributedeventing.codec.kafka.TestUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class CreateTopicsResponseTest {

    @Test
    public void testV5_1() {
        String responseHex = "00 00 00 04 00 00 00 00 00 02 0b 63 6f 64 65 63 2d 74 65 73 74 00 00 00 00 00 00 01 " +
                             "00 01 1b 11 63 6f 6d 70 72 65 73 73 69 6f 6e 2e 74 79 70 65 09 70 72 6f 64 75 63 65 " +
                             "72 00 05 00 00 26 6c 65 61 64 65 72 2e 72 65 70 6c 69 63 61 74 69 6f 6e 2e 74 68 72 " +
                             "6f 74 74 6c 65 64 2e 72 65 70 6c 69 63 61 73 01 00 05 00 00 14 6d 69 6e 2e 69 6e 73 " +
                             "79 6e 63 2e 72 65 70 6c 69 63 61 73 02 31 00 05 00 00 1e 6d 65 73 73 61 67 65 2e 64 " +
                             "6f 77 6e 63 6f 6e 76 65 72 73 69 6f 6e 2e 65 6e 61 62 6c 65 05 74 72 75 65 00 05 00 " +
                             "00 12 73 65 67 6d 65 6e 74 2e 6a 69 74 74 65 72 2e 6d 73 02 30 00 05 00 00 0f 63 6c " +
                             "65 61 6e 75 70 2e 70 6f 6c 69 63 79 07 64 65 6c 65 74 65 00 05 00 00 09 66 6c 75 73 " +
                             "68 2e 6d 73 14 39 32 32 33 33 37 32 30 33 36 38 35 34 37 37 35 38 30 37 00 05 00 00 " +
                             "28 66 6f 6c 6c 6f 77 65 72 2e 72 65 70 6c 69 63 61 74 69 6f 6e 2e 74 68 72 6f 74 74 " +
                             "6c 65 64 2e 72 65 70 6c 69 63 61 73 01 00 05 00 00 0e 73 65 67 6d 65 6e 74 2e 62 79 " +
                             "74 65 73 0b 31 30 37 33 37 34 31 38 32 34 00 04 00 00 0d 72 65 74 65 6e 74 69 6f 6e " +
                             "2e 6d 73 0a 36 30 34 38 30 30 30 30 30 00 05 00 00 0f 66 6c 75 73 68 2e 6d 65 73 73 " +
                             "61 67 65 73 14 39 32 32 33 33 37 32 30 33 36 38 35 34 37 37 35 38 30 37 00 05 00 00 " +
                             "17 6d 65 73 73 61 67 65 2e 66 6f 72 6d 61 74 2e 76 65 72 73 69 6f 6e 08 32 2e 35 2d " +
                             "49 56 30 00 05 00 00 16 6d 61 78 2e 63 6f 6d 70 61 63 74 69 6f 6e 2e 6c 61 67 2e 6d " +
                             "73 14 39 32 32 33 33 37 32 30 33 36 38 35 34 37 37 35 38 30 37 00 05 00 00 15 66 69 " +
                             "6c 65 2e 64 65 6c 65 74 65 2e 64 65 6c 61 79 2e 6d 73 06 36 30 30 30 30 00 05 00 00 " +
                             "12 6d 61 78 2e 6d 65 73 73 61 67 65 2e 62 79 74 65 73 08 31 30 34 38 35 38 38 00 05 " +
                             "00 00 16 6d 69 6e 2e 63 6f 6d 70 61 63 74 69 6f 6e 2e 6c 61 67 2e 6d 73 02 30 00 05 " +
                             "00 00 17 6d 65 73 73 61 67 65 2e 74 69 6d 65 73 74 61 6d 70 2e 74 79 70 65 0b 43 72 " +
                             "65 61 74 65 54 69 6d 65 00 05 00 00 0c 70 72 65 61 6c 6c 6f 63 61 74 65 06 66 61 6c " +
                             "73 65 00 05 00 00 15 69 6e 64 65 78 2e 69 6e 74 65 72 76 61 6c 2e 62 79 74 65 73 05 " +
                             "34 30 39 36 00 05 00 00 1a 6d 69 6e 2e 63 6c 65 61 6e 61 62 6c 65 2e 64 69 72 74 79 " +
                             "2e 72 61 74 69 6f 04 30 2e 35 00 05 00 00 1f 75 6e 63 6c 65 61 6e 2e 6c 65 61 64 65 " +
                             "72 2e 65 6c 65 63 74 69 6f 6e 2e 65 6e 61 62 6c 65 06 66 61 6c 73 65 00 05 00 00 10 " +
                             "72 65 74 65 6e 74 69 6f 6e 2e 62 79 74 65 73 03 2d 31 00 05 00 00 14 64 65 6c 65 74 " +
                             "65 2e 72 65 74 65 6e 74 69 6f 6e 2e 6d 73 09 38 36 34 30 30 30 30 30 00 05 00 00 0b " +
                             "73 65 67 6d 65 6e 74 2e 6d 73 0a 36 30 34 38 30 30 30 30 30 00 05 00 00 24 6d 65 73 " +
                             "73 61 67 65 2e 74 69 6d 65 73 74 61 6d 70 2e 64 69 66 66 65 72 65 6e 63 65 2e 6d 61 " +
                             "78 2e 6d 73 14 39 32 32 33 33 37 32 30 33 36 38 35 34 37 37 35 38 30 37 00 05 00 00 " +
                             "14 73 65 67 6d 65 6e 74 2e 69 6e 64 65 78 2e 62 79 74 65 73 09 31 30 34 38 35 37 36 " +
                             "30 00 05 00 00 00 00";
        ByteBuf responseBuffer = TestUtils.hexToBinary(responseHex);
        CreateTopicsResponse response = CreateTopicsResponse.getInstance((short) 5);
        try {
            response.readFrom(responseBuffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        assertEquals(4, response.header().correlationId().value());
        assertEquals(0, response.throttleTimeMs().value());
        Array<CreatableTopicResult>.ElementIterator topics = response.topics();
        assertTrue(topics.hasNext());
        CreatableTopicResult topic = topics.next();
        assertFalse(topics.hasNext());
        assertEquals("codec-test", topic.name().valueAsString());
        assertEquals(0, topic.errorCode().value());
        assertTrue(topic.errorMessage().isNull());
        assertEquals(1, topic.numPartitions().value());
        assertEquals(1, topic.replicationFactor().value());
        Array<CreatableTopicConfigs>.ElementIterator configs = topic.configs();
        assertTrue(configs.hasNext());
        assertConfigsV5_1(configs);
        assertFalse(configs.hasNext());
        assertFalse(topic.taggedFields().hasNext());
        assertFalse(response.taggedFields().hasNext());
    }

    private void assertConfigsV5_1(Array<CreatableTopicConfigs>.ElementIterator configs) {
        assertConfigProperties(configs.next(), "compression.type", "producer", 5);
        assertConfigProperties(configs.next(), "leader.replication.throttled.replicas", "", 5);
        assertConfigProperties(configs.next(), "min.insync.replicas", "1", 5);
        assertConfigProperties(configs.next(), "message.downconversion.enable", "true", 5);
        assertConfigProperties(configs.next(), "segment.jitter.ms", "0", 5);
        assertConfigProperties(configs.next(), "cleanup.policy", "delete", 5);
        assertConfigProperties(configs.next(), "flush.ms", "9223372036854775807", 5);
        assertConfigProperties(configs.next(), "follower.replication.throttled.replicas", "", 5);
        assertConfigProperties(configs.next(), "segment.bytes", "1073741824", 4);
        assertConfigProperties(configs.next(), "retention.ms", "604800000", 5);
        assertConfigProperties(configs.next(), "flush.messages", "9223372036854775807", 5);
        assertConfigProperties(configs.next(), "message.format.version", "2.5-IV0", 5);
        assertConfigProperties(configs.next(), "max.compaction.lag.ms", "9223372036854775807", 5);
        assertConfigProperties(configs.next(), "file.delete.delay.ms", "60000", 5);
        assertConfigProperties(configs.next(), "max.message.bytes", "1048588", 5);
        assertConfigProperties(configs.next(), "min.compaction.lag.ms", "0", 5);
        assertConfigProperties(configs.next(), "message.timestamp.type", "CreateTime", 5);
        assertConfigProperties(configs.next(), "preallocate", "false", 5);
        assertConfigProperties(configs.next(), "index.interval.bytes", "4096", 5);
        assertConfigProperties(configs.next(), "min.cleanable.dirty.ratio", "0.5", 5);
        assertConfigProperties(configs.next(), "unclean.leader.election.enable", "false", 5);
        assertConfigProperties(configs.next(), "retention.bytes", "-1", 5);
        assertConfigProperties(configs.next(), "delete.retention.ms", "86400000", 5);
        assertConfigProperties(configs.next(), "segment.ms", "604800000", 5);
        assertConfigProperties(configs.next(), "message.timestamp.difference.max.ms", "9223372036854775807", 5);
        assertConfigProperties(configs.next(), "segment.index.bytes", "10485760", 5);
    }

    private void assertConfigProperties(CreatableTopicConfigs config,
                                        String name,
                                        String value,
                                        int configSource) {
        assertEquals(name, config.name().valueAsString());
        assertEquals(value, config.value().valueAsString());
        assertFalse(config.readOnly().value());
        assertEquals(configSource, config.configSource().value());
        assertFalse(config.isSensitive().value());
        assertFalse(config.taggedFields().hasNext());
    }
}