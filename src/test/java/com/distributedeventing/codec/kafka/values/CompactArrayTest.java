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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class CompactArrayTest {

    @Test
    public void testWriteTo() {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            ArrayTestUtils.testWriteToUsing(new CompactArray<Int32>(Int32.FACTORY), random);
        }
        // test empty array
        ArrayTestUtils.testWriteToUsing(new CompactArray<Int32>(Int32.FACTORY), new int[] {});
    }

    @Test
    public void testReadFrom() {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            ArrayTestUtils.testReadFromUsing(new CompactArray<Int32>(Int32.FACTORY), random);
        }
        // test empty array
        ArrayTestUtils.testReadFromUsing(new CompactArray<Int32>(Int32.FACTORY), new int[] {});
    }

    @Test
    public void testSizeOfInBytes() {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            ArrayTestUtils.testSizeOfInBytesUsing(new CompactArray<Int32>(Int32.FACTORY), random);
        }
        // test empty array
        ArrayTestUtils.testSizeOfInBytesUsing(new CompactArray<Int32>(Int32.FACTORY), new int[] {});
    }

    @Test
    public void testAccessorsAndMutators() {
        Random random = ThreadLocalRandom.current();
        Array<Int32> arr = new CompactArray<>(Int32.FACTORY);
        for (int i=0; i<100; ++i) {
            ArrayTestUtils.testAccessorsAndMutatorsUsing(arr, random);
        }
        // test empty array
        ArrayTestUtils.testAccessorsAndMutatorsUsing(arr, new int[] {});
        arr.value();
        assertEquals(0, arr.length());
        ArrayTestUtils.testAdditionToEmptyArrayUsing(random, arr);
    }

    @Test
    public void testReset() {
        ArrayTestUtils.testResetUsing(new CompactArray<>(Boolean.FACTORY));
    }

    @Test
    public void testAppendTo() {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            ArrayTestUtils.testAppendToUsing(new CompactArray<Int32>(Int32.FACTORY), random);
        }
        // test empty array
        ArrayTestUtils.testAppendToUsing(new CompactArray<Int32>(Int32.FACTORY), new int[] {});
    }

    @Test
    public void testInvalidReadLength() {
        ArrayTestUtils.testInvalidReadLengthUsing(-1, new CompactArray<Int32>(Int32.FACTORY));
    }
}