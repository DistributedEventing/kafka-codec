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
package com.distributedeventing.codec.kafka.messages.requests;

public class ConsumerDefaults {

    /** Replica ID of a consumer **/
    public static final int CONSUMER_REPLICA_ID = -1;

    public static enum IsolationLevel {

        /**
         * If the isolation level of a request is set to this, all available
         * records will be considered by the broker. For example, a fetch
         * request with read uncommitted isolation level will return all
         * records.
         */
        READ_UNCOMMITTED(0),

        /**
         * If the isolation level of a request is set to this, only non-transactional
         * and committed transactional records will be considered by the broker.
         * Read committed refers to records with offsets smaller than the LSO
         * (last stable offset).
         */
        READ_COMMITTED(1);

        private final byte level;

        public byte level() {
            return level;
        }

        private IsolationLevel(final int level) {
            this.level = (byte) level;
        }
    }
}
