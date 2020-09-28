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

public class Int16Test {

    @Test
    public void testWriteTo() {
        testWriteOf(Short.MIN_VALUE);
        testWriteOf((short) (Short.MIN_VALUE/2));
        testWriteOf((short) 0);
        testWriteOf((short) (Short.MAX_VALUE/2));
        testWriteOf(Short.MAX_VALUE);
    }

    @Test
    public void testReadFrom() {
        testReadOf(Short.MIN_VALUE);
        testReadOf((short) (Short.MIN_VALUE/2));
        testReadOf((short) 0);
        testReadOf((short) (Short.MAX_VALUE/2));
        testReadOf(Short.MAX_VALUE);
    }

    @Test
    public void testSizeInBytes() {
        Int16 value = new Int16();
        assertEquals(2, value.sizeInBytes());
        testSizeInBytes(value, Short.MIN_VALUE);
        testSizeInBytes(value, (short) (Short.MIN_VALUE/2));
        testSizeInBytes(value, (short) 0);
        testSizeInBytes(value, (short) (Short.MAX_VALUE/2));
        testSizeInBytes(value, Short.MAX_VALUE);
    }

    @Test
    public void testAccessorAndMutator() {
        Int16 value = new Int16();
        assertEquals(0, value.value());
        testAccessorAndMutatorUsing(value, Short.MIN_VALUE);
        testAccessorAndMutatorUsing(value, (short) (Short.MIN_VALUE/2));
        testAccessorAndMutatorUsing(value, (short) 0);
        testAccessorAndMutatorUsing(value, (short) (Short.MAX_VALUE/2));
        testAccessorAndMutatorUsing(value, Short.MAX_VALUE);
    }

    @Test
    public void testAppendTo() {
        StringBuilder sb = new StringBuilder();
        Int16 value = new Int16();
        testAppendUsing(value, sb, Short.MIN_VALUE);
        testAppendUsing(value, sb, (short) (Short.MIN_VALUE/2));
        testAppendUsing(value, sb, (short) 0);
        testAppendUsing(value, sb, (short) (Short.MAX_VALUE/2));
        testAppendUsing(value, sb, Short.MAX_VALUE);
    }

    @Test
    public void testReset() {
        short s = 2;
        Int16 value = new Int16(s);
        assertEquals(s, value.value());
        value.reset();
        assertEquals(0, value.value());
    }

    @Test
    public void testFactory() {
        ValueFactory<Int16> factory = Int16.FACTORY;
        Value instance = factory.createInstance();
        assertTrue(instance instanceof Int16);
    }

    private void testAppendUsing(Int16 value,
                                 StringBuilder sb,
                                 short s) {
        value.value(s);
        sb.setLength(0);
        value.appendTo(sb);
        assertEquals(java.lang.String.valueOf(s), sb.toString());
    }

    private void testAccessorAndMutatorUsing(Int16 value,
                                             short s) {
        value.value(s);
        assertEquals(s, value.value());
    }

    private void testSizeInBytes(Int16 value,
                                 short s) {
        value.value(s);
        assertEquals(2, value.sizeInBytes());
    }

    private void testReadOf(short s) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeShort(s);
        Int16 value = new Int16();
        value.readFrom(buffer);
        assertEquals(s, value.value());
    }

    private void testWriteOf(short s) {
        Int16 value = new Int16(s);
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        assertEquals(2, buffer.readableBytes());
        assertEquals(s, buffer.readShort());
    }
}