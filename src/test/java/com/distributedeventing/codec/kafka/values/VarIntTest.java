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
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class VarIntTest {


    @Test
    public void testWriteTo() {
        testWriteOf(Integer.MIN_VALUE);
        testWriteOf(0);
        testWriteOf(Integer.MAX_VALUE);
    }

    @Test
    public void testReadFrom() {
        try {
            testReadOf(Integer.MIN_VALUE);
            testReadOf(0);
            testReadOf(Integer.MAX_VALUE);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSizeInBytes() {
        VarInt value = new VarInt();
        Assert.assertEquals(ByteUtils.sizeOfVarInt(0), value.sizeInBytes());
        testSizeInBytes(value, Integer.MIN_VALUE);
        testSizeInBytes(value, 0);
        testSizeInBytes(value, Integer.MAX_VALUE);
    }

    @Test
    public void testAccessorAndMutator() {
        VarInt value = new VarInt();
        assertEquals(0, value.value());
        testAccessorAndMutatorUsing(value, Integer.MIN_VALUE);
        testAccessorAndMutatorUsing(value, 0);
        testAccessorAndMutatorUsing(value, Integer.MAX_VALUE);
    }

    @Test
    public void testAppendTo() {
        StringBuilder sb = new StringBuilder();
        VarInt value = new VarInt();
        testAppendUsing(value, sb, Integer.MIN_VALUE);
        testAppendUsing(value, sb, 0);
        testAppendUsing(value, sb, Integer.MAX_VALUE);
    }

    @Test
    public void testReset() {
        int i = 3;
        VarInt value = new VarInt(i);
        assertEquals(i, value.value());
        value.reset();
        assertEquals(0, value.value());
    }

    @Test
    public void testFactory() {
        ValueFactory<VarInt> factory = VarInt.FACTORY;
        Value instance = factory.createInstance();
        assertTrue(instance instanceof VarInt);
    }

    private void testWriteOf(int i) {
        VarInt value = new VarInt(i);
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        assertEquals(ByteUtils.sizeOfVarInt(i), buffer.readableBytes());
        assertEquals(i, ByteUtils.readVarInt(buffer));
    }

    private void testReadOf(int i) throws ParseException {
        ByteBuf buffer = Unpooled.buffer();
        ByteUtils.writeVarInt(i, buffer);
        VarInt value = new VarInt();
        value.readFrom(buffer);
        assertEquals(i, value.value());
    }

    private void testSizeInBytes(VarInt value,
                                 int i) {
        value.value(i);
        assertEquals(ByteUtils.sizeOfVarInt(i), value.sizeInBytes());
    }

    private void testAccessorAndMutatorUsing(VarInt value,
                                             int i) {
        value.value(i);
        assertEquals(i, value.value());
    }

    private void testAppendUsing(VarInt value,
                                 StringBuilder sb,
                                 int i) {
        value.value(i);
        sb.setLength(0);
        value.appendTo(sb);
        assertEquals(java.lang.String.valueOf(i), sb.toString());
    }
}