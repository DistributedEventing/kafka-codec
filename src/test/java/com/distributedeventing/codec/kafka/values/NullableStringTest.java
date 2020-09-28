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

public class NullableStringTest {

    @Test
    public void testWriteTo() {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            StringTestUtils.testWriteUsing(random, new NullableString(), random.nextInt(Short.MAX_VALUE), 2);
        }
        // test length = 0
        StringTestUtils.testWriteUsing(random, new NullableString(), 0, 2);
    }

    @Test
    public void testWriteOfNull() {
        NullableString value = new NullableString();
        value.nullify();
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        assertEquals(2, buffer.readableBytes());
        assertEquals(-1, buffer.readShort());
    }

    @Test
    public void testReadFrom() {
        Random random = ThreadLocalRandom.current();
        ByteBuf buffer = Unpooled.buffer();
        NullableString value = new NullableString();
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
        buffer.writeShort(-1);
        NullableString value = new NullableString();
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
        NullableString value = new NullableString();
        assertEquals(2, value.sizeInBytes());
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            StringTestUtils.testSizeInBytesUsing(random.nextInt(Short.MAX_VALUE), 2, value, random);
        }
        value.nullify();
        assertEquals(2, value.sizeInBytes());
        value.empty();
        assertEquals(2, value.sizeInBytes());
    }

    @Test
    public void testAccessorsAndMutators() {
        NullableString value = new NullableString(1, Scaler.DEFAULT_SCALER);
        assertEquals(0, value.length());
        assertEquals(2, value.sizeInBytes());
        assertFalse(value.isNull());

        value.value((byte[]) null);
        assertEquals(-1, value.length());
        assertEquals(2, value.sizeInBytes());
        assertTrue(value.isNull());

        value.value(StringTestUtils.TEST_STRING);
        Assert.assertEquals(StringTestUtils.TEST_STRING.length(), value.length());
        Assert.assertEquals(StringTestUtils.TEST_STRING, value.valueAsString());

        value.value((java.lang.String) null);
        assertEquals(-1, value.length());
        assertEquals(2, value.sizeInBytes());
        assertTrue(value.isNull());

        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            StringTestUtils.testAccessorsAndMutatorsUsing(value, random);
        }

        value.value(new byte[0]);
        assertEquals(0, value.length());
        assertEquals(2, value.sizeInBytes());
        assertFalse(value.isNull());

        byte[] strBytes = StringTestUtils.TEST_STRING.getBytes(ByteUtils.CHARSET_UTF8);
        value.value(strBytes, 0, strBytes.length);
        assertEquals(strBytes.length, value.length());
        byte[] bytesRead = new byte[value.length()];
        System.arraycopy(value.value(), 0, bytesRead, 0, value.length());
        assertArrayEquals(strBytes, bytesRead);
        Assert.assertEquals(StringTestUtils.TEST_STRING, value.valueAsString());

        value.value(null, 0, 0);
        assertEquals(-1, value.length());
        assertEquals(2, value.sizeInBytes());
        assertTrue(value.isNull());
        assertNull(value.valueAsString());
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