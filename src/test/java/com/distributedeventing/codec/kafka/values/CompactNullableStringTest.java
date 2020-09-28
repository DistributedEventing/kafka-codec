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

import com.distributedeventing.codec.kafka.ByteUtils;
import com.distributedeventing.codec.kafka.exceptions.ParseException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.distributedeventing.codec.kafka.Scaler;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class CompactNullableStringTest {

    private static final int COMPACT_SIZE_OF_LENGTH_MINUS_1 = ByteUtils.sizeOfUnsignedVarInt(0);

    @Test
    public void testWriteTo() {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            int length = random.nextInt(Short.MAX_VALUE);
            int lenOfLen = ByteUtils.sizeOfUnsignedVarInt(length + 1);
            StringTestUtils.testWriteUsing(random, new CompactNullableString(), length, lenOfLen);
        }
        // test length = 0
        StringTestUtils.testWriteUsing(random, new CompactNullableString(), 0, StringTestUtils.COMPACT_SIZE_OF_LENGTH_0);
    }

    @Test
    public void testWriteOfNull() {
        CompactNullableString value = new CompactNullableString();
        value.nullify();
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        assertEquals(COMPACT_SIZE_OF_LENGTH_MINUS_1, buffer.readableBytes());
        assertEquals(0, ByteUtils.readUnsignedVarInt(buffer));
    }

    @Test
    public void testReadFrom() {
        Random random = ThreadLocalRandom.current();
        ByteBuf buffer = Unpooled.buffer();
        CompactNullableString value = new CompactNullableString();
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
    public void testReadOfNull() {
        ByteBuf buffer = Unpooled.buffer();
        ByteUtils.writeUnsignedVarInt(0, buffer);
        CompactNullableString value = new CompactNullableString();
        try {
            value.readFrom(buffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        assertTrue(value.isNull());
        assertEquals(-1, value.length());
    }

    @Test
    public void testSizeInBytes() {
        CompactNullableString value = new CompactNullableString();
        assertEquals(ByteUtils.sizeOfUnsignedVarInt(1), value.sizeInBytes());
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            int length = random.nextInt(Short.MAX_VALUE);
            int lenOfLen = ByteUtils.sizeOfUnsignedVarInt(length + 1);
            StringTestUtils.testSizeInBytesUsing(length, lenOfLen, value, random);
        }
        value.nullify();
        assertEquals(COMPACT_SIZE_OF_LENGTH_MINUS_1, value.sizeInBytes());
        value.empty();
        assertEquals(StringTestUtils.COMPACT_SIZE_OF_LENGTH_0, value.sizeInBytes());
    }

    @Test
    public void testAccessorsAndMutators() {
        CompactNullableString value = new CompactNullableString(1, Scaler.DEFAULT_SCALER);

        value.value((byte[]) null);
        assertTrue(value.isNull());
        assertNull(value.valueAsString());
        assertEquals(-1, value.length());
        assertEquals(COMPACT_SIZE_OF_LENGTH_MINUS_1, value.sizeInBytes());

        value.value(StringTestUtils.TEST_STRING);
        assertEquals(StringTestUtils.TEST_STRING.length(), value.length());
        Assert.assertEquals(StringTestUtils.TEST_STRING, value.valueAsString());
        assertFalse(value.isNull());

        value.value((java.lang.String) null);
        assertTrue(value.isNull());
        assertNull(value.valueAsString());
        assertEquals(-1, value.length());
        assertEquals(COMPACT_SIZE_OF_LENGTH_MINUS_1, value.sizeInBytes());

        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            StringTestUtils.testAccessorsAndMutatorsUsing(value, random);
            assertFalse(value.isNull());
        }

        value.value(new byte[0]);
        assertEquals(0, value.length());
        assertEquals(StringTestUtils.COMPACT_SIZE_OF_LENGTH_0, value.sizeInBytes());
        assertFalse(value.isNull());

        byte[] strBytes = StringTestUtils.TEST_STRING.getBytes(ByteUtils.CHARSET_UTF8);
        value.value(strBytes, 0, strBytes.length);
        assertEquals(strBytes.length, value.length());
        byte[] bytesRead = new byte[value.length()];
        System.arraycopy(value.value(), 0, bytesRead, 0, value.length());
        assertArrayEquals(strBytes, bytesRead);
        Assert.assertEquals(StringTestUtils.TEST_STRING, value.valueAsString());
        assertFalse(value.isNull());

        value.value(null, 0, 0);
        assertTrue(value.isNull());
        assertNull(value.valueAsString());
        assertEquals(-1, value.length());
        assertEquals(COMPACT_SIZE_OF_LENGTH_MINUS_1, value.sizeInBytes());
    }

    @Test
    public void testInvalidLengthStrings() {
        StringTestUtils.testInvalidLengthStringsUsing(new CompactNullableString());
    }

    @Test
    public void testInvalidLengthRead() {
        StringTestUtils.testInvalidLengthReadUsing(new CompactNullableString());
    }

    private void testReadUsing(Random random,
                               ByteBuf buffer,
                               int length,
                               CompactNullableString value) throws ParseException {
        buffer.clear();
        ByteUtils.writeUnsignedVarInt(length + 1, buffer);
        StringTestUtils.testReadUsing(random, length, buffer, value);
    }
}