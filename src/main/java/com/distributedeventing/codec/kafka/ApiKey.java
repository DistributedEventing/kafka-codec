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
package com.distributedeventing.codec.kafka;

import com.distributedeventing.codec.kafka.values.Int32;

import java.util.HashMap;
import java.util.Map;

public enum ApiKey {

    PRODUCE(0),
    FETCH(1),
    LIST_OFFSETS(2),
    METADATA(3),
    LEADER_AND_ISR(4),
    STOP_REPLICA(5),
    UPDATE_METADATA(6),
    CONTROLLED_SHUTDOWN(7),
    OFFSET_COMMIT(8),
    OFFSET_FETCH(9),
    FIND_COORDINATOR(10),
    JOIN_GROUP(11),
    HEARTBEAT(12),
    LEAVE_GROUP(13),
    SYNC_GROUP(14),
    DESCRIBE_GROUPS(15),
    LIST_GROUPS(16),
    SASL_HANDSHAKE(17),
    API_VERSIONS(18),
    CREATE_TOPICS(19),
    DELETE_TOPICS(20),
    DELETE_RECORDS(21),
    INIT_PRODUCER_ID(22),
    OFFSET_FOR_LEADER_EPOCH(23),
    ADD_PARTITIONS_TO_TXN(24),
    ADD_OFFSETS_TO_TXN(25),
    END_TXN(26),
    WRITE_TXN_MARKERS(27),
    TXN_OFFSET_COMMIT(28),
    DESCRIBE_ACLS(29),
    CREATE_ACLS(30),
    DELETE_ACLS(31),
    DESCRIBE_CONFIGS(32),
    ALTER_CONFIGS(33),
    ALTER_REPLICA_LOG_DIRS(34),
    DESCRIBE_LOG_DIRS(35),
    SASL_AUTHENTICATE(36),
    CREATE_PARTITIONS(37),
    CREATE_DELEGATION_TOKEN(38),
    RENEW_DELEGATION_TOKEN(39),
    EXPIRE_DELEGATION_TOKEN(40),
    DESCRIBE_DELEGATION_TOKEN(41),
    DELETE_GROUPS(42),
    ELECT_LEADERS(43),
    INCREMENTAL_ALTER_CONFIGS(44),
    ALTER_PARTITION_REASSIGNMENTS(45),
    LIST_PARTITION_REASSIGNMENTS(46),
    OFFSET_DELETE(47),
    DESCRIBE_CLIENT_QUOTAS(48),
    ALTER_CLIENT_QUOTAS(49);

    private static final Map<Int32, ApiKey> KEY_MAP = new HashMap<>();
    static {
        for (final ApiKey apiKey: ApiKey.values()) {
            KEY_MAP.put(new Int32(apiKey.key), apiKey);
        }
    }

    private static final Int32 SCRATCH_INT = new Int32();

    private final short key;

    public static ApiKey getKey(final int key) {
        SCRATCH_INT.value(key);
        return KEY_MAP.get(SCRATCH_INT);
    }

    public short key() {
        return key;
    }

    private ApiKey(final int key) {
        this.key = (short) key;
    }
}