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

import com.distributedeventing.codec.kafka.exceptions.ParseException;
import org.junit.Test;

public class CompactBytesTest {


    @Test
    public void testWriteTo() {
        BytesTestUtils.testWriteToUsing(new CompactBytes());
    }

    @Test
    public void testReadFrom() {
        BytesTestUtils.testReadFromUsing(new CompactBytes());
    }

    @Test
    public void testSizeInBytes() {
        BytesTestUtils.testSizeInBytesUsing(new CompactBytes());
    }

    @Test
    public void testAccessorsAndMutators() {
        BytesTestUtils.testAccessorsAndMutatorsUsing(new CompactBytes());
    }

    @Test
    public void testReset() {
        BytesTestUtils.testResetUsing(new CompactBytes());
    }

    @Test
    public void testAppendTo() {
        BytesTestUtils.testAppendToUsing(new CompactBytes());
    }

    @Test(expected = ParseException.class)
    public void testNegativeLength() throws ParseException {
        BytesTestUtils.testNegativeLengthUsing(new CompactBytes());
    }
}