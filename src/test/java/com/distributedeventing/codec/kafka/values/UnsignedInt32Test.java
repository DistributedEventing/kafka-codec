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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class UnsignedInt32Test {

    @Test
    public void testWriteTo() {
        testWriteOf(0);
        testWriteOf(Integer.MAX_VALUE/2);
        testWriteOf(Integer.MAX_VALUE/2 + 1);
        testWriteOf(Integer.MAX_VALUE);
    }

    @Test
    public void testReadFrom() {
        testReadOf(0);
        testReadOf(Integer.MAX_VALUE/2);
        testReadOf(Integer.MAX_VALUE/2 + 1);
        testReadOf(Integer.MAX_VALUE);
    }

    @Test
    public void testSizeInBytes() {
        UnsignedInt32 value = new UnsignedInt32();
        assertEquals(4, value.sizeInBytes());
        testSizeInBytes(value, 0);
        testSizeInBytes(value, Integer.MAX_VALUE/2);
        testSizeInBytes(value, Integer.MAX_VALUE/2 + 1);
        testSizeInBytes(value, Integer.MAX_VALUE);
    }

    @Test
    public void testAccessorAndMutator() {
        UnsignedInt32 value = new UnsignedInt32();
        assertEquals(0, value.value());
        testAccessorAndMutatorUsing(value, 0);
        testAccessorAndMutatorUsing(value, Integer.MAX_VALUE/2);
        testAccessorAndMutatorUsing(value, Integer.MAX_VALUE/2 + 1);
        testAccessorAndMutatorUsing(value, Integer.MAX_VALUE);
    }

    @Test
    public void testAppendTo() {
        StringBuilder sb = new StringBuilder();
        UnsignedInt32 value = new UnsignedInt32();
        testAppendUsing(value, sb, 0);
        testAppendUsing(value, sb, Integer.MAX_VALUE/2);
        testAppendUsing(value, sb, Integer.MAX_VALUE/2 + 1);
        testAppendUsing(value, sb, Integer.MAX_VALUE);
    }

    @Test
    public void testReset() {
        int i = 3;
        UnsignedInt32 value = new UnsignedInt32(i);
        assertEquals(i, value.value());
        value.reset();
        assertEquals(0, value.value());
    }

    @Test
    public void testFactory() {
        ValueFactory<UnsignedInt32> factory = UnsignedInt32.FACTORY;
        Value instance = factory.createInstance();
        assertTrue(instance instanceof UnsignedInt32);
    }

    private void testSizeInBytes(UnsignedInt32 value,
                                 int i) {
        value.value(i);
        assertEquals(4, value.sizeInBytes());
    }

    private void testWriteOf(int i) {
        UnsignedInt32 value = new UnsignedInt32(i);
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        assertEquals(4, buffer.readableBytes());
        Assert.assertEquals(i, ByteUtils.readUnsignedInt(buffer));
    }

    private void testReadOf(int i) {
        ByteBuf buffer = Unpooled.buffer();
        ByteUtils.writeUnsignedInt(i, buffer);
        UnsignedInt32 value = new UnsignedInt32();
        value.readFrom(buffer);
        assertEquals(i, value.value());
    }

    private void testAccessorAndMutatorUsing(UnsignedInt32 value,
                                             int i) {
        value.value(i);
        assertEquals(i, value.value());
    }

    private void testAppendUsing(UnsignedInt32 value,
                                 StringBuilder sb,
                                 int i) {
        value.value(i);
        sb.setLength(0);
        value.appendTo(sb);
        assertEquals(java.lang.String.valueOf(i), sb.toString());
    }
}