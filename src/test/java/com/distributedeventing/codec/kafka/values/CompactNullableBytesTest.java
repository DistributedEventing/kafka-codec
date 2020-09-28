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
package com.distributedeventing.codec.kafka.values;

import org.junit.Test;

public class CompactNullableBytesTest {

    @Test
    public void testWriteTo() {
        BytesTestUtils.testWriteToUsing(new CompactNullableBytes());
    }

    @Test
    public void testWriteOfNull() {
        BytesTestUtils.testWriteOfNullUsing(new CompactNullableBytes());
    }

    @Test
    public void testReadFrom() {
        BytesTestUtils.testReadFromUsing(new CompactNullableBytes());
    }

    @Test
    public void testReadOfNull() {
        BytesTestUtils.testReadOfNullUsing(new CompactNullableBytes());
    }

    @Test
    public void testSizeInBytes() {
        BytesTestUtils.testSizeInBytesUsing(new CompactNullableBytes());
    }

    @Test
    public void testAccessorsAndMutators() {
        BytesTestUtils.testAccessorsAndMutatorsUsing(new CompactNullableBytes());
    }

    @Test
    public void testReset() {
        BytesTestUtils.testResetUsing(new CompactNullableBytes());
    }

    @Test
    public void testAppendTo() {
        BytesTestUtils.testAppendToUsing(new CompactNullableBytes());
    }
}