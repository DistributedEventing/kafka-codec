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

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class Int32Test {

    @Test
    public void testWriteTo() {
        testWriteOf(Integer.MIN_VALUE);
        testWriteOf(Integer.MIN_VALUE/2);
        testWriteOf(0);
        testWriteOf(Integer.MAX_VALUE/2);
        testWriteOf(Integer.MAX_VALUE);
    }

    @Test
    public void testReadFrom() {
        testReadOf(Integer.MIN_VALUE);
        testReadOf(Integer.MIN_VALUE/2);
        testReadOf(0);
        testReadOf(Integer.MAX_VALUE/2);
        testReadOf(Integer.MAX_VALUE);
    }

    @Test
    public void testSizeInBytes() {
        Int32 value = new Int32();
        assertEquals(4, value.sizeInBytes());
        testSizeInBytes(value, Integer.MIN_VALUE);
        testSizeInBytes(value, Integer.MIN_VALUE/2);
        testSizeInBytes(value, 0);
        testSizeInBytes(value, Integer.MAX_VALUE/2);
        testSizeInBytes(value, Integer.MAX_VALUE);
    }

    @Test
    public void testAccessorAndMutator() {
        Int32 value = new Int32();
        assertEquals(0, value.value());
        testAccessorAndMutatorUsing(value, Integer.MIN_VALUE);
        testAccessorAndMutatorUsing(value, Integer.MIN_VALUE/2);
        testAccessorAndMutatorUsing(value, 0);
        testAccessorAndMutatorUsing(value, Integer.MAX_VALUE/2);
        testAccessorAndMutatorUsing(value, Integer.MAX_VALUE);
    }

    @Test
    public void testAppendTo() {
        StringBuilder sb = new StringBuilder();
        Int32 value = new Int32();
        testAppendUsing(value, sb, Integer.MIN_VALUE);
        testAppendUsing(value, sb, Integer.MIN_VALUE/2);
        testAppendUsing(value, sb, 0);
        testAppendUsing(value, sb, Integer.MAX_VALUE/2);
        testAppendUsing(value, sb, Integer.MAX_VALUE);
    }

    @Test
    public void testReset() {
        int i = 4;
        Int32 value = new Int32(i);
        assertEquals(i, value.value());
        value.reset();
        assertEquals(0, value.value());
    }

    @Test
    public void testFactory() {
        ValueFactory<Int32> factory = Int32.FACTORY;
        Value instance = factory.createInstance();
        assertTrue(instance instanceof Int32);
    }

    @Test
    public void testEquals() {
        int i = 125;
        Int32 one = new Int32(i);
        assertTrue(one.equals(one));
        assertFalse(one.equals(null));
        assertFalse(one.equals(new Int16((short) i)));
        Int32 two = new Int32(i);
        assertTrue(one.equals(two));
        Int32 three = new Int32(126);
        assertFalse(one.equals(three));
    }

    @Test
    public void testHashCode() {
        int i = 125;
        Int32 one = new Int32(i);
        Int32 two = new Int32(i);
        Map<Int32, java.lang.String> m = new HashMap<>();
        java.lang.String data = "data";
        m.put(one, data);
        assertTrue(m.containsKey(one));
        assertFalse(m.containsKey(null));
        assertEquals(data, m.get(two));
        Int32 three = new Int32(126);
        assertFalse(m.containsKey(three));
    }

    private void testWriteOf(int i) {
        Int32 value = new Int32(i);
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        assertEquals(4, buffer.readableBytes());
        assertEquals(i, buffer.readInt());
    }

    private void testReadOf(int i) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeInt(i);
        Int32 value = new Int32();
        value.readFrom(buffer);
        assertEquals(i, value.value());
    }

    private void testSizeInBytes(Int32 value,
                                 int i) {
        value.value(i);
        assertEquals(4, value.sizeInBytes());
    }

    private void testAccessorAndMutatorUsing(Int32 value,
                                             int i) {
        value.value(i);
        assertEquals(i, value.value());
    }

    private void testAppendUsing(Int32 value,
                                 StringBuilder sb,
                                 int i) {
        value.value(i);
        sb.setLength(0);
        value.appendTo(sb);
        assertEquals(java.lang.String.valueOf(i), sb.toString());
    }
}