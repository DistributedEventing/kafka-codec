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
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class CompactStringTest {


    @Test
    public void testWriteTo() {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            int length = random.nextInt(Short.MAX_VALUE);
            int lenOfLen = ByteUtils.sizeOfUnsignedVarInt(length + 1);
            StringTestUtils.testWriteUsing(random, new CompactString(), length, lenOfLen);
        }
        // test length = 0
        StringTestUtils.testWriteUsing(random, new CompactString(), 0, StringTestUtils.COMPACT_SIZE_OF_LENGTH_0);
    }

    @Test
    public void testReadFrom() {
        Random random = ThreadLocalRandom.current();
        ByteBuf buffer = Unpooled.buffer();
        CompactString value = new CompactString();
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
        CompactString value = new CompactString();
        assertEquals(ByteUtils.sizeOfUnsignedVarInt(1), value.sizeInBytes());
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            int length = random.nextInt(Short.MAX_VALUE);
            int lenOfLen = ByteUtils.sizeOfUnsignedVarInt(length + 1);
            StringTestUtils.testSizeInBytesUsing(length, lenOfLen, value, random);
        }
    }

    @Test
    public void testAccessorsAndMutators() {
        CompactString value = new CompactString(1, Scaler.DEFAULT_SCALER);

        value.value((byte[]) null);
        assertEquals(0, value.length());
        assertEquals(StringTestUtils.COMPACT_SIZE_OF_LENGTH_0, value.sizeInBytes());

        value.value(StringTestUtils.TEST_STRING);
        assertEquals(StringTestUtils.TEST_STRING.length(), value.length());
        assertEquals(StringTestUtils.TEST_STRING, value.valueAsString());

        value.value((java.lang.String) null);
        assertEquals(0, value.length());
        assertEquals(StringTestUtils.COMPACT_SIZE_OF_LENGTH_0, value.sizeInBytes());

        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            StringTestUtils.testAccessorsAndMutatorsUsing(value, random);
        }

        value.value(new byte[0]);
        assertEquals(0, value.length());
        assertEquals(StringTestUtils.COMPACT_SIZE_OF_LENGTH_0, value.sizeInBytes());

        byte[] strBytes = StringTestUtils.TEST_STRING.getBytes(ByteUtils.CHARSET_UTF8);
        value.value(strBytes, 0, strBytes.length);
        assertEquals(strBytes.length, value.length());
        byte[] bytesRead = new byte[value.length()];
        System.arraycopy(value.value(), 0, bytesRead, 0, value.length());
        assertArrayEquals(strBytes, bytesRead);
        assertEquals(StringTestUtils.TEST_STRING, value.valueAsString());

        value.value(null, 0, 0);
        assertEquals(0, value.length());
        assertEquals(StringTestUtils.COMPACT_SIZE_OF_LENGTH_0, value.sizeInBytes());
    }

    @Test
    public void testReset() {
        CompactString value = new CompactString();
        value.value(StringTestUtils.TEST_STRING);
        assertEquals(StringTestUtils.TEST_STRING.length(), value.length());
        assertEquals(ByteUtils.sizeOfUnsignedVarInt(StringTestUtils.TEST_STRING.length()) + StringTestUtils.TEST_STRING.length(),
                     value.sizeInBytes());
        value.reset();
        assertEquals(0, value.length());
        assertEquals(StringTestUtils.COMPACT_SIZE_OF_LENGTH_0, value.sizeInBytes());
    }


    @Test
    public void testAppendTo() {
        StringTestUtils.testAppendToUsing(new CompactString());
    }

    @Test
    public void testInvalidLengthStrings() {
        StringTestUtils.testInvalidLengthStringsUsing(new CompactString());
    }

    @Test
    public void testInvalidLengthRead() {
        StringTestUtils.testInvalidLengthReadUsing(new CompactString());
    }

    @Test
    public void testFactory() {
        ValueFactory<CompactString> factory = new CompactString.CompactStringFactory();
        Value instance = factory.createInstance();
        assertTrue(instance instanceof CompactString);
    }

    private void testReadUsing(Random random,
                               ByteBuf buffer,
                               int length,
                               CompactString value) throws ParseException {
        buffer.clear();
        ByteUtils.writeUnsignedVarInt(length + 1, buffer);
        StringTestUtils.testReadUsing(random, length, buffer, value);
    }

}