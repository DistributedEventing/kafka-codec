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

/**
 * A codec that can be used by an application built using Netty
 * to communicate with an Apache Kafka cluster.
 *
 * The codec has been designed for single threaded use and supports
 * the following APIs of Kafka:
 * <ul>
 *     <li>Produce</li>
 *     <li>Fetch</li>
 *     <li>List offsets</li>
 *     <li>Metadata</li>
 *     <li>API versions</li>
 *     <li>Create topics</li>
 *     <li>Delete topics</li>
 * </ul>
 *
 * All of Kafka's APIs are versioned. But this codec supports only few
 * versions (the latest ones at the time of this writing).
 *
 * This codec intentionally omits support for the following:
 * <u>
 *     <li>transactions</li>
 *     <li>consumer groups</li>
 *     <li>idempotence</li>
 *     <li>fetch sessions</li>
 * </u>
 *
 * Sample usage of the codec is provided as a {@link com.distributedeventing.codec.kafka.Demo}.
 */
package com.distributedeventing.codec.kafka;