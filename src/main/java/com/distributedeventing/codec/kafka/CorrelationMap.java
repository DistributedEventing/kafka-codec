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

/**
 * A map that is used to correlate requests to and responses from
 * Kafka brokers.
 */
public final class CorrelationMap {

    private int correlationId = 0;

    private final Map<Int32, ApiType> map = new HashMap<>();

    private final Int32 key = new Int32();

    /**
     * Gets the next available correlation ID. This has the
     * effect of creating an association between a correlation
     * ID and an {@link ApiType}.
     */
    public int getNextCorrelationId(final ApiType apiType) {
        map.put(new Int32(correlationId), apiType);
        return correlationId++;
    }

    /**
     * Gets the {@link ApiType} associated with a correlation ID.
     * This has the effect of removing the association between
     * correlation ID and {@link ApiType}.
     */
    public ApiType getApiType(final int correlationId) {
        key.value(correlationId);
        return map.remove(key);
    }
}