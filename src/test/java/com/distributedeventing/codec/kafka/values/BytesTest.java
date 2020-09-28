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
import com.distributedeventing.codec.kafka.Scaler;
import org.junit.Test;

public class BytesTest {

    @Test
    public void testWriteTo() {
        BytesTestUtils.testWriteToUsing(new Bytes());
    }

    @Test
    public void testReadFrom() {
        BytesTestUtils.testReadFromUsing(new Bytes());
    }

    @Test
    public void testSizeInBytes() {
        BytesTestUtils.testSizeInBytesUsing(new Bytes());
    }

    @Test
    public void testAccessorsAndMutators() {
        BytesTestUtils.testAccessorsAndMutatorsUsing(new Bytes());
    }

    @Test
    public void testReset() {
        BytesTestUtils.testResetUsing(new Bytes());
    }

    @Test
    public void testAppendTo() {
        BytesTestUtils.testAppendToUsing(new Bytes());
    }

    @Test(expected = ParseException.class)
    public void testNegativeLength() throws ParseException {
        BytesTestUtils.testNegativeLengthUsing(new Bytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalConstruction1() {
        new Bytes(0, Scaler.DEFAULT_SCALER);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalConstruction2() {
        new Bytes(1, null);
    }
}