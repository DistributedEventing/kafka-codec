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

public class BooleanTest {

    private static final byte TRUE = 1;

    private static final byte FALSE = 0;

    @Test
    public void testWriteTo() {
        testWriteOf(true);
        testWriteOf(false);
    }

    @Test
    public void testReadFrom() {
        testReadOf(true);
        testReadOf(false);
    }

    @Test
    public void testSizeInBytes() {
        Boolean value = new Boolean();
        assertEquals(1, value.sizeInBytes());
        testSizeInBytes(value, true);
        testSizeInBytes(value, false);
    }

    @Test
    public void testAccessorAndMutator() {
        Boolean value = new Boolean();
        assertEquals(false, value.value());
        testAccessorAndMutatorUsing(value, true);
        testAccessorAndMutatorUsing(value, false);
    }

    @Test
    public void testAppendTo() {
        StringBuilder sb = new StringBuilder();
        Boolean value = new Boolean();
        testAppendUsing(value, sb, true);
        testAppendUsing(value, sb, false);
    }

    @Test
    public void testReset() {
        boolean b = true;
        Boolean value = new Boolean(b);
        assertEquals(b, value.value());
        value.reset();
        assertEquals(false, value.value());
    }

    @Test
    public void testFactory() {
        ValueFactory<Boolean> factory = Boolean.FACTORY;
        Value instance = factory.createInstance();
        assertTrue(instance instanceof Boolean);
    }

    private void testWriteOf(boolean b) {
        Boolean value = new Boolean(b);
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        assertEquals(1, buffer.readableBytes());
        assertEquals(b? TRUE: FALSE, buffer.readByte());
    }

    private void testReadOf(boolean b) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte(b? TRUE: FALSE);
        Boolean value = new Boolean();
        value.readFrom(buffer);
        assertEquals(b, value.value());
    }

    private void testSizeInBytes(Boolean value,
                                 boolean b) {
        value.value(b);
        assertEquals(1, value.sizeInBytes());
    }

    private void testAccessorAndMutatorUsing(Boolean value,
                                             boolean b) {
        value.value(b);
        assertEquals(b, value.value());
    }

    private void testAppendUsing(Boolean value,
                                 StringBuilder sb,
                                 boolean b) {
        value.value(b);
        sb.setLength(0);
        value.appendTo(sb);
        assertEquals(java.lang.String.valueOf(b), sb.toString());
    }
}