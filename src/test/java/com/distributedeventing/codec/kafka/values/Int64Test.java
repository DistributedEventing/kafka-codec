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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.*;

public class Int64Test {

    @Test
    public void testWriteTo() {
        testWriteOf(Long.MIN_VALUE);
        testWriteOf(Long.MIN_VALUE/2);
        testWriteOf(0);
        testWriteOf(Long.MAX_VALUE/2);
        testWriteOf(Long.MAX_VALUE);
    }

    @Test
    public void testReadFrom() {
        testReadOf(Long.MIN_VALUE);
        testReadOf(Long.MIN_VALUE/2);
        testReadOf(0);
        testReadOf(Long.MAX_VALUE/2);
        testReadOf(Long.MAX_VALUE);
    }

    @Test
    public void testSizeInBytes() {
        Int64 value = new Int64();
        assertEquals(8, value.sizeInBytes());
        testSizeInBytes(value, Long.MIN_VALUE);
        testSizeInBytes(value, Long.MIN_VALUE/2);
        testSizeInBytes(value, 0);
        testSizeInBytes(value, Long.MAX_VALUE/2);
        testSizeInBytes(value, Long.MAX_VALUE);
    }

    @Test
    public void testAppendTo() {
        StringBuilder sb = new StringBuilder();
        Int64 value = new Int64();
        testAppendUsing(value, sb, Long.MIN_VALUE);
        testAppendUsing(value, sb, Long.MIN_VALUE/2);
        testAppendUsing(value, sb, 0);
        testAppendUsing(value, sb, Long.MAX_VALUE/2);
        testAppendUsing(value, sb, Long.MAX_VALUE);
    }

    @Test
    public void testReset() {
        long l = 8;
        Int64 value = new Int64(l);
        assertEquals(l, value.value());
        value.reset();
        assertEquals(0, value.value());
    }

    @Test
    public void testFactory() {
        ValueFactory<Int64> factory = Int64.FACTORY;
        Value instance = factory.createInstance();
        assertTrue(instance instanceof Int64);
    }

    private void testSizeInBytes(Int64 value,
                                 long l) {
        value.value(l);
        assertEquals(8, value.sizeInBytes());
    }

    private void testWriteOf(long l) {
        Int64 value = new Int64(l);
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        assertEquals(8, buffer.readableBytes());
        assertEquals(l, buffer.readLong());
    }

    private void testReadOf(long l) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeLong(l);
        Int64 value = new Int64();
        value.readFrom(buffer);
        assertEquals(l, value.value());
    }

    private void testAppendUsing(Int64 value,
                                 StringBuilder sb,
                                 long l) {
        value.value(l);
        sb.setLength(0);
        value.appendTo(sb);
        assertEquals(java.lang.String.valueOf(l), sb.toString());
    }
}