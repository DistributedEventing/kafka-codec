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
import com.distributedeventing.codec.kafka.ByteUtils;
import com.distributedeventing.codec.kafka.exceptions.ParseException;
import org.junit.Test;

import static org.junit.Assert.*;

public class TaggedBytesTest {

    private static final int TAG = 128;

    private static final byte[] DATA = "topics".getBytes(ByteUtils.CHARSET_UTF8);

    @Test
    public void testWriteTo() {
        TaggedBytes value = new TaggedBytes();
        value.tag(TAG);
        value.value(DATA);
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        ByteBuf expectedBuffer = getExpectedBuffer(TAG, DATA);
        assertTrue(expectedBuffer.equals(buffer));
    }

    @Test
    public void testReadFrom() {
        ByteBuf buffer = getExpectedBuffer(TAG, DATA);
        TaggedBytes value = new TaggedBytes();
        try {
            value.readFrom(buffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        assertEquals(TAG, value.tag());
        byte[] readData = new byte[value.length()];
        System.arraycopy(value.value(), 0, readData, 0, value.length());
        assertArrayEquals(DATA, readData);
    }

    @Test
    public void testSizeInBytes() {
        TaggedBytes value = new TaggedBytes();
        value.tag(TAG);
        value.value(DATA);
        ByteBuf buffer = getExpectedBuffer(TAG, DATA);
        assertEquals(buffer.readableBytes(), value.sizeInBytes());
    }

    @Test
    public void testFactory() {
        ValueFactory<TaggedBytes> factory = new TaggedBytes.TaggedBytesFactory();
        assertTrue(factory.createInstance() instanceof TaggedBytes);
    }

    private ByteBuf getExpectedBuffer(int tag,
                                      byte[] data) {
        ByteBuf buffer = Unpooled.buffer();
        ByteUtils.writeUnsignedVarInt(tag, buffer);
        ByteUtils.writeUnsignedVarInt(data.length, buffer);
        buffer.writeBytes(data);
        return buffer;
    }

}