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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.distributedeventing.codec.kafka.ByteUtils;
import com.distributedeventing.codec.kafka.Scaler;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static com.distributedeventing.codec.kafka.values.StringTestUtils.TEST_STRING;
import static org.junit.Assert.*;

public class StringTest {

    @Test
    public void testWriteTo() {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            StringTestUtils.testWriteUsing(random, new String(), random.nextInt(Short.MAX_VALUE), 2);
        }
        // test length = 0
        StringTestUtils.testWriteUsing(random, new String(), 0, 2);
    }

    @Test
    public void testReadFrom() {
        Random random = ThreadLocalRandom.current();
        ByteBuf buffer = Unpooled.buffer();
        String value = new String();
        try {
            for (int i=0; i<100; ++i) {
                int length = random.nextInt(Short.MAX_VALUE);
                testReadUsing(random, buffer, length, value);
            }
            // test length = 0
            testReadUsing(random, buffer, 0, value);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSizeInBytes() {
        String value = new String();
        assertEquals(2, value.sizeInBytes());
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            StringTestUtils.testSizeInBytesUsing(random.nextInt(Short.MAX_VALUE), 2, value, random);
        }
    }

    @Test
    public void testAccessorsAndMutators() {
        String value = new String(1, Scaler.DEFAULT_SCALER);

        value.value((byte[]) null);
        assertEquals(0, value.length());
        assertEquals(2, value.sizeInBytes());

        value.value(TEST_STRING);
        assertEquals(TEST_STRING.length(), value.length());
        assertEquals(TEST_STRING, value.valueAsString());

        value.value((java.lang.String) null);
        assertEquals(0, value.length());
        assertEquals(2, value.sizeInBytes());

        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            StringTestUtils.testAccessorsAndMutatorsUsing(value, random);
        }

        value.value(new byte[0]);
        assertEquals(0, value.length());
        assertEquals(2, value.sizeInBytes());

        byte[] strBytes = TEST_STRING.getBytes(ByteUtils.CHARSET_UTF8);
        value.value(strBytes, 0, strBytes.length);
        assertEquals(strBytes.length, value.length());
        byte[] bytesRead = new byte[value.length()];
        System.arraycopy(value.value(), 0, bytesRead, 0, value.length());
        assertArrayEquals(strBytes, bytesRead);
        assertEquals(TEST_STRING, value.valueAsString());

        value.value(null, 0, 0);
        assertEquals(0, value.length());
        assertEquals(2, value.sizeInBytes());
    }

    @Test
    public void testReset() {
        String value = new String();
        value.value(TEST_STRING);
        assertEquals(TEST_STRING.length(), value.length());
        assertEquals(2 + TEST_STRING.length(), value.sizeInBytes());
        value.reset();
        assertEquals(0, value.length());
        assertEquals(2, value.sizeInBytes());
    }

    @Test
    public void testAppendTo() {
        StringTestUtils.testAppendToUsing(new String());
    }

    @Test
    public void testInvalidLengthStrings() {
        StringTestUtils.testInvalidLengthStringsUsing(new String());
    }

    @Test
    public void testInvalidLengthRead() {
        StringTestUtils.testInvalidLengthReadUsing(new String());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalConstruction1() {
        new String(-1, Scaler.DEFAULT_SCALER);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalConstruction2() {
        new String(Short.MAX_VALUE + 1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalConstruction3() {
        new String(1, null);
    }

    private void testReadUsing(Random random,
                               ByteBuf buffer,
                               int length,
                               String value) throws ParseException {
        buffer.clear();
        buffer.writeShort(length);
        StringTestUtils.testReadUsing(random, length, buffer, value);
    }
}