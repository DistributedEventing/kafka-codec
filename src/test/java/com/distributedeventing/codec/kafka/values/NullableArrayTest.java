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

public class NullableArrayTest {

    @Test
    public void testWriteTo() {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            ArrayTestUtils.testWriteToUsing(new NullableArray<Int32>(Int32.FACTORY), random);
        }
        // test empty array
        ArrayTestUtils.testWriteToUsing(new NullableArray<Int32>(Int32.FACTORY), new int[] {});
    }

    @Test
    public void testWriteOfNull() {
        ArrayTestUtils.testWriteOfNullUsing(new NullableArray<>(Int32.FACTORY));
    }

    @Test
    public void testReadFrom() {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            ArrayTestUtils.testReadFromUsing(new NullableArray<Int32>(Int32.FACTORY), random);
        }
        // test empty array
        ArrayTestUtils.testReadFromUsing(new NullableArray<Int32>(Int32.FACTORY), new int[] {});
    }

    @Test
    public void testReadOfNull() {
        ArrayTestUtils.testReadOfNullUsing(new NullableArray<Int32>(Int32.FACTORY));
    }

    @Test
    public void testSizeOfInBytes() {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            ArrayTestUtils.testSizeOfInBytesUsing(new NullableArray<Int32>(Int32.FACTORY), random);
        }
        // test empty array
        ArrayTestUtils.testSizeOfInBytesUsing(new NullableArray<Int32>(Int32.FACTORY), new int[] {});
    }

    @Test
    public void testSizeOfNullableInBytes() {
        NullableArray<Int32> arr = new NullableArray<>(Int32.FACTORY);
        arr.nullify();
        ArrayTestUtils.testSizeOfNullableInBytesUsing(arr);
    }

    @Test
    public void testAccessorsAndMutators() {
        Random random = ThreadLocalRandom.current();
        NullableArray<Int32> arr = new NullableArray<>(Int32.FACTORY);
        for (int i=0; i<100; ++i) {
            ArrayTestUtils.testAccessorsAndMutatorsUsing(arr, random);
        }
        // test null array
        arr.value((Int32[]) null);
        assertTrue(arr.isNull());
        assertEquals(-1, arr.length());
        // test empty array
        ArrayTestUtils.testAccessorsAndMutatorsUsing(arr, new int[] {});
        arr.value();
        assertEquals(0, arr.length());
        ArrayTestUtils.testAdditionToEmptyArrayUsing(random, arr);
        arr.nullify();
        assertTrue(arr.isNull());
        assertEquals(-1, arr.length());
        arr.empty();
        assertFalse(arr.isNull());
        assertEquals(0, arr.length());
    }

    @Test
    public void testReset() {
        ArrayTestUtils.testResetUsing(new NullableArray<>(Boolean.FACTORY));
    }

    @Test
    public void testAppendTo() {
        Random random = ThreadLocalRandom.current();
        NullableArray<Int32> arr = new NullableArray<>(Int32.FACTORY);
        for (int i = 0; i<100; ++i) {
            ArrayTestUtils.testAppendToUsing(arr, random);
        }
        // test empty array
        ArrayTestUtils.testAppendToUsing(arr, new int[] {});
        // test null array
        arr.nullify();
        ArrayTestUtils.testAppendToOfNullArrayUsing(arr);
    }
}