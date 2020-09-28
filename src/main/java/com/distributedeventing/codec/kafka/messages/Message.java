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
package com.distributedeventing.codec.kafka.messages;

import com.distributedeventing.codec.kafka.Schema;

/**
 * A request to a Kafka broker or a response from a
 * Kafka broker.
 */
public interface Message {

    /**
     * Gets the schema of the message.
     */
    Schema schema();

    /**
     * Gets the size of the message, in bytes, on the wire.
     */
    int sizeInBytes();

    /**
     * Resets the message for reuse.
     */
    void reset();

    /**
     * Appends a human readable form of the message to the
     * supplied {@link StringBuilder}.
     */
    StringBuilder appendTo(StringBuilder sb);
}