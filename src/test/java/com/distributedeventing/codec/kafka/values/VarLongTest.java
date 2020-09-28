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

public class VarLongTest {

    @Test
    public void testWriteTo() {
        testWriteOf(Long.MIN_VALUE);
        testWriteOf(Long.MIN_VALUE/2);
        testWriteOf(Long.MIN_VALUE/2 + 1);
        testWriteOf(0);
        testWriteOf(Long.MAX_VALUE/2);
        testWriteOf(Long.MAX_VALUE/2 + 1);
        testWriteOf(Long.MAX_VALUE);
    }

    @Test
    public void testReadFrom() {
        try {
            testReadOf(Long.MIN_VALUE);
            testReadOf(Long.MIN_VALUE/2);
            testReadOf(Long.MIN_VALUE/2 + 1);
            testReadOf(0);
            testReadOf(Long.MAX_VALUE/2);
            testReadOf(Long.MAX_VALUE/2 + 1);
            testReadOf(Long.MAX_VALUE);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSizeInBytes() {
        VarLong value = new VarLong();
        Assert.assertEquals(ByteUtils.sizeOfVarLong(0), value.sizeInBytes());
        testSizeInBytes(value, Long.MIN_VALUE);
        testSizeInBytes(value, Long.MIN_VALUE/2);
        testSizeInBytes(value, Long.MIN_VALUE/2 + 1);
        testSizeInBytes(value, 0);
        testSizeInBytes(value, Long.MAX_VALUE/2);
        testSizeInBytes(value, Long.MAX_VALUE/2 + 1);
        testSizeInBytes(value, Long.MAX_VALUE);
    }

    @Test
    public void testAccessorAndMutator() {
        VarLong value = new VarLong();
        assertEquals(0, value.value());
        testAccessorAndMutatorUsing(value, Long.MIN_VALUE);
        testAccessorAndMutatorUsing(value, Long.MIN_VALUE/2);
        testAccessorAndMutatorUsing(value, Long.MIN_VALUE/2 + 1);
        testAccessorAndMutatorUsing(value, 0);
        testAccessorAndMutatorUsing(value, Long.MAX_VALUE/2);
        testAccessorAndMutatorUsing(value, Long.MAX_VALUE/2 + 1);
        testAccessorAndMutatorUsing(value, Long.MAX_VALUE);
    }

    @Test
    public void testAppendTo() {
        StringBuilder sb = new StringBuilder();
        VarLong value = new VarLong();
        testAppendUsing(value, sb, Long.MIN_VALUE);
        testAppendUsing(value, sb, Long.MIN_VALUE/2);
        testAppendUsing(value, sb, Long.MIN_VALUE/2 + 1);
        testAppendUsing(value, sb, 0);
        testAppendUsing(value, sb, Long.MAX_VALUE/2);
        testAppendUsing(value, sb, Long.MAX_VALUE/2 + 1);
        testAppendUsing(value, sb, Long.MAX_VALUE);
    }

    @Test
    public void testReset() {
        long l = 5;
        VarLong value = new VarLong(l);
        assertEquals(l, value.value());
        value.reset();
        assertEquals(0, value.value());
    }

    @Test
    public void testFactory() {
        ValueFactory<VarLong> factory = VarLong.FACTORY;
        Value instance = factory.createInstance();
        assertTrue(instance instanceof VarLong);
    }

    private void testWriteOf(long l) {
        VarLong value = new VarLong(l);
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        assertEquals(ByteUtils.sizeOfVarLong(l), buffer.readableBytes());
        assertEquals(l, ByteUtils.readVarlong(buffer));
    }

    private void testReadOf(long l) throws ParseException {
        ByteBuf buffer = Unpooled.buffer();
        ByteUtils.writeVarlong(l, buffer);
        VarLong value = new VarLong();
        value.readFrom(buffer);
        assertEquals(l, value.value());
    }

    private void testSizeInBytes(VarLong value,
                                 long l) {
        value.value(l);
        assertEquals(ByteUtils.sizeOfVarLong(l), value.sizeInBytes());
    }

    private void testAccessorAndMutatorUsing(VarLong value,
                                             long l) {
        value.value(l);
        assertEquals(l, value.value());
    }

    private void testAppendUsing(VarLong value,
                                 StringBuilder sb,
                                 long l) {
        value.value(l);
        sb.setLength(0);
        value.appendTo(sb);
        assertEquals(java.lang.String.valueOf(l), sb.toString());
    }
}