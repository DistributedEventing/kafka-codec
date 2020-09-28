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
package com.distributedeventing.codec.kafka.messages.responses.apiversions;

import com.distributedeventing.codec.kafka.ApiKey;
import com.distributedeventing.codec.kafka.exceptions.ParseException;
import com.distributedeventing.codec.kafka.values.Array;
import io.netty.buffer.ByteBuf;
import com.distributedeventing.codec.kafka.TestUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class ApiVersionsResponseTest {

    @Test
    public void testV3_1() {
        String responseHex = "00 00 00 02 00 00 31 00 00 00 00 00 08 00 00 01 00 00 00 0b 00 00 02 00 00 00 05 00 " +
                             "00 03 00 00 00 09 00 00 04 00 00 00 04 00 00 05 00 00 00 02 00 00 06 00 00 00 06 00 " +
                             "00 07 00 00 00 03 00 00 08 00 00 00 08 00 00 09 00 00 00 07 00 00 0a 00 00 00 03 00 " +
                             "00 0b 00 00 00 07 00 00 0c 00 00 00 04 00 00 0d 00 00 00 04 00 00 0e 00 00 00 05 00 " +
                             "00 0f 00 00 00 05 00 00 10 00 00 00 03 00 00 11 00 00 00 01 00 00 12 00 00 00 03 00 " +
                             "00 13 00 00 00 05 00 00 14 00 00 00 04 00 00 15 00 00 00 01 00 00 16 00 00 00 03 00 " +
                             "00 17 00 00 00 03 00 00 18 00 00 00 01 00 00 19 00 00 00 01 00 00 1a 00 00 00 01 00 " +
                             "00 1b 00 00 00 00 00 00 1c 00 00 00 03 00 00 1d 00 00 00 02 00 00 1e 00 00 00 02 00 " +
                             "00 1f 00 00 00 02 00 00 20 00 00 00 02 00 00 21 00 00 00 01 00 00 22 00 00 00 01 00 " +
                             "00 23 00 00 00 01 00 00 24 00 00 00 02 00 00 25 00 00 00 02 00 00 26 00 00 00 02 00 " +
                             "00 27 00 00 00 02 00 00 28 00 00 00 02 00 00 29 00 00 00 02 00 00 2a 00 00 00 02 00 " +
                             "00 2b 00 00 00 02 00 00 2c 00 00 00 01 00 00 2d 00 00 00 00 00 00 2e 00 00 00 00 00 " +
                             "00 2f 00 00 00 00 00 00 00 00 00 00";
        ByteBuf responseBuffer = TestUtils.hexToBinary(responseHex);
        ApiVersionsResponse response = ApiVersionsResponse.getInstance((short) 3);
        try {
            response.readFrom(responseBuffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        assertEquals(2, response.header().correlationId().value());
        assertEquals(0, response.errorCode().value());
        Array<ApiVersionsResponseKey>.ElementIterator apiKeys = response.apiKeys();
        testApiKeysV3_1(apiKeys);
        assertEquals(0, response.throttleTimeMs().value());
        assertFalse(response.taggedFields().hasNext());
    }

    private void testApiKeysV3_1(Array<ApiVersionsResponseKey>.ElementIterator apiKeys) {
        testApiKey(apiKeys.next(), ApiKey.PRODUCE, 0, 8);
        testApiKey(apiKeys.next(), ApiKey.FETCH, 0, 11);
        testApiKey(apiKeys.next(), ApiKey.LIST_OFFSETS, 0, 5);
        testApiKey(apiKeys.next(), ApiKey.METADATA, 0, 9);
        testApiKey(apiKeys.next(), ApiKey.LEADER_AND_ISR, 0, 4);
        testApiKey(apiKeys.next(), ApiKey.STOP_REPLICA, 0, 2);
        testApiKey(apiKeys.next(), ApiKey.UPDATE_METADATA, 0, 6);
        testApiKey(apiKeys.next(), ApiKey.CONTROLLED_SHUTDOWN, 0, 3);
        testApiKey(apiKeys.next(), ApiKey.OFFSET_COMMIT, 0, 8);
        testApiKey(apiKeys.next(), ApiKey.OFFSET_FETCH, 0, 7);
        testApiKey(apiKeys.next(), ApiKey.FIND_COORDINATOR, 0, 3);
        testApiKey(apiKeys.next(), ApiKey.JOIN_GROUP, 0, 7);
        testApiKey(apiKeys.next(), ApiKey.HEARTBEAT, 0, 4);
        testApiKey(apiKeys.next(), ApiKey.LEAVE_GROUP, 0, 4);
        testApiKey(apiKeys.next(), ApiKey.SYNC_GROUP, 0, 5);
        testApiKey(apiKeys.next(), ApiKey.DESCRIBE_GROUPS, 0, 5);
        testApiKey(apiKeys.next(), ApiKey.LIST_GROUPS, 0, 3);
        testApiKey(apiKeys.next(), ApiKey.SASL_HANDSHAKE, 0, 1);
        testApiKey(apiKeys.next(), ApiKey.API_VERSIONS, 0, 3);
        testApiKey(apiKeys.next(), ApiKey.CREATE_TOPICS, 0, 5);
        testApiKey(apiKeys.next(), ApiKey.DELETE_TOPICS, 0, 4);
        testApiKey(apiKeys.next(), ApiKey.DELETE_RECORDS, 0, 1);
        testApiKey(apiKeys.next(), ApiKey.INIT_PRODUCER_ID, 0, 3);
        testApiKey(apiKeys.next(), ApiKey.OFFSET_FOR_LEADER_EPOCH, 0, 3);
        testApiKey(apiKeys.next(), ApiKey.ADD_PARTITIONS_TO_TXN, 0, 1);
        testApiKey(apiKeys.next(), ApiKey.ADD_OFFSETS_TO_TXN, 0, 1);
        testApiKey(apiKeys.next(), ApiKey.END_TXN, 0, 1);
        testApiKey(apiKeys.next(), ApiKey.WRITE_TXN_MARKERS, 0, 0);
        testApiKey(apiKeys.next(), ApiKey.TXN_OFFSET_COMMIT, 0, 3);
        testApiKey(apiKeys.next(), ApiKey.DESCRIBE_ACLS, 0, 2);
        testApiKey(apiKeys.next(), ApiKey.CREATE_ACLS, 0, 2);
        testApiKey(apiKeys.next(), ApiKey.DELETE_ACLS, 0, 2);
        testApiKey(apiKeys.next(), ApiKey.DESCRIBE_CONFIGS, 0, 2);
        testApiKey(apiKeys.next(), ApiKey.ALTER_CONFIGS, 0, 1);
        testApiKey(apiKeys.next(), ApiKey.ALTER_REPLICA_LOG_DIRS, 0, 1);
        testApiKey(apiKeys.next(), ApiKey.DESCRIBE_LOG_DIRS, 0, 1);
        testApiKey(apiKeys.next(), ApiKey.SASL_AUTHENTICATE, 0, 2);
        testApiKey(apiKeys.next(), ApiKey.CREATE_PARTITIONS, 0, 2);
        testApiKey(apiKeys.next(), ApiKey.CREATE_DELEGATION_TOKEN, 0, 2);
        testApiKey(apiKeys.next(), ApiKey.RENEW_DELEGATION_TOKEN, 0, 2);
        testApiKey(apiKeys.next(), ApiKey.EXPIRE_DELEGATION_TOKEN, 0, 2);
        testApiKey(apiKeys.next(), ApiKey.DESCRIBE_DELEGATION_TOKEN, 0, 2);
        testApiKey(apiKeys.next(), ApiKey.DELETE_GROUPS, 0, 2);
        testApiKey(apiKeys.next(), ApiKey.ELECT_LEADERS, 0, 2);
        testApiKey(apiKeys.next(), ApiKey.INCREMENTAL_ALTER_CONFIGS, 0, 1);
        testApiKey(apiKeys.next(), ApiKey.ALTER_PARTITION_REASSIGNMENTS, 0, 0);
        testApiKey(apiKeys.next(), ApiKey.LIST_PARTITION_REASSIGNMENTS, 0, 0);
        testApiKey(apiKeys.next(), ApiKey.OFFSET_DELETE, 0, 0);
    }

    private void testApiKey(ApiVersionsResponseKey key,
                            ApiKey expectedKey,
                            int expectedMinVersion,
                            int expectedMaxVersion) {
        assertEquals(expectedKey.key(), key.apiKey().value());
        assertEquals(expectedMinVersion, key.minVersion().value());
        assertEquals(expectedMaxVersion, key.maxVersion().value());
        assertFalse(key.taggedFields().hasNext());
    }
}