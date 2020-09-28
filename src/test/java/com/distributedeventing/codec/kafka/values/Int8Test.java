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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Int8Test {

    @Test
    public void testWriteTo() {
        testWriteOf(Byte.MIN_VALUE);
        testWriteOf((byte) 0);
        testWriteOf(Byte.MAX_VALUE);
    }

    @Test
    public void testReadFrom() {
        testReadOf(Byte.MIN_VALUE);
        testReadOf((byte) 0);
        testReadOf(Byte.MAX_VALUE);
    }

    @Test
    public void testSizeInBytes() {
        Int8 value = new Int8();
        assertEquals(1, value.sizeInBytes());
        testSizeInBytes(value, Byte.MIN_VALUE);
        testSizeInBytes(value, (byte) 0);
        testSizeInBytes(value, Byte.MAX_VALUE);
    }

    @Test
    public void testAccessorAndMutator() {
        Int8 value = new Int8();
        assertEquals(0, value.value());
        testAccessorAndMutatorUsing(value, Byte.MIN_VALUE);
        testAccessorAndMutatorUsing(value, (byte) 0);
        testAccessorAndMutatorUsing(value, Byte.MAX_VALUE);
    }

    @Test
    public void testAppendTo() {
        StringBuilder sb = new StringBuilder();
        Int8 value = new Int8();
        testAppendUsing(value, sb, Byte.MIN_VALUE);
        testAppendUsing(value, sb, (byte) 0);
        testAppendUsing(value, sb, Byte.MAX_VALUE);
    }

    @Test
    public void testReset() {
        byte b = (byte) 1;
        Int8 value = new Int8(b);
        assertEquals(b, value.value());
        value.reset();
        assertEquals(0, value.value());
    }

    @Test
    public void testFactory() {
        ValueFactory<Int8> factory = Int8.FACTORY;
        Value instance = factory.createInstance();
        assertTrue(instance instanceof Int8);
    }

    private void testAppendUsing(Int8 value,
                                 StringBuilder sb,
                                 byte b) {
        value.value(b);
        sb.setLength(0);
        value.appendTo(sb);
        assertEquals(java.lang.String.valueOf(b), sb.toString());
    }

    private void testAccessorAndMutatorUsing(Int8 value,
                                             byte b) {
        value.value(b);
        assertEquals(b, value.value());
    }

    private void testSizeInBytes(Int8 value,
                                 byte b) {
        value.value(b);
        assertEquals(1, value.sizeInBytes());
    }

    private void testWriteOf(byte b) {
        Int8 value = new Int8(b);
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        assertEquals(1, buffer.readableBytes());
        assertEquals(b, buffer.readByte());
    }

    private void testReadOf(byte b) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte(b);
        Int8 value = new Int8();
        value.readFrom(buffer);
        assertEquals(b, value.value());
    }
}