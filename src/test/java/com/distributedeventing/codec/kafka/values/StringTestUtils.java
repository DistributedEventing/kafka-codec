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

import com.distributedeventing.codec.kafka.exceptions.ParseException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.distributedeventing.codec.kafka.ByteUtils;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class StringTestUtils {

    public static final java.lang.String TEST_STRING = "netty kafka codec";

    public static final int COMPACT_SIZE_OF_LENGTH_0 = ByteUtils.sizeOfUnsignedVarInt(1);

    public static void testWriteUsing(Random random,
                                      String value,
                                      int length,
                                      int lenOfLen) {
        byte[] bytesToWrite = new byte[length];
        random.nextBytes(bytesToWrite);
        value.value(bytesToWrite);
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        assertEquals(lenOfLen + length, buffer.readableBytes());
        byte[] bytesRead = new byte[length];
        buffer.readerIndex(lenOfLen);
        buffer.readBytes(bytesRead);
        assertArrayEquals(bytesToWrite, bytesRead);
    }

    public static void testReadUsing(Random random,
                                     int length,
                                     ByteBuf bufWithLenWritten,
                                     String value) throws ParseException {
        byte[] bytesToWrite = new byte[length];
        random.nextBytes(bytesToWrite);
        bufWithLenWritten.writeBytes(bytesToWrite);
        value.readFrom(bufWithLenWritten);
        assertEquals(length, value.length());
        byte[] bytesRead = new byte[value.length()];
        System.arraycopy(value.value(), 0, bytesRead, 0, value.length());
        assertArrayEquals(bytesToWrite, bytesRead);
    }

    public static void testSizeInBytesUsing(int length,
                                            int lenOfLen,
                                            String value,
                                            Random random) {
        byte[] bytesToWrite = new byte[length];
        value.value(bytesToWrite);
        assertEquals(lenOfLen + length, value.sizeInBytes());
    }

    public static void testAccessorsAndMutatorsUsing(String value,
                                                     Random random) {
        int length = random.nextInt(Short.MAX_VALUE);
        byte[] bytesToWrite = new byte[length];
        random.nextBytes(bytesToWrite);
        value.value(bytesToWrite);
        assertEquals(length, value.length());
        byte[] bytesRead = new byte[value.length()];
        System.arraycopy(value.value(), 0, bytesRead, 0, value.length());
        assertArrayEquals(bytesToWrite, bytesRead);
    }

    public static void testAppendToUsing(java.lang.String str,
                                         String value,
                                         StringBuilder sb) {
        sb.setLength(0);
        value.value(str);
        value.appendTo(sb);
        assertEquals(str, sb.toString());
    }

    public static void testInvalidLengthUsing(Object data,
                                              String value) {
        try {
            if (data instanceof java.lang.String) {
                value.value((java.lang.String) data);
            } else {
                value.value((byte[]) data);
            }
            fail("Expected illegal argument exception");
        } catch (IllegalArgumentException ex) {
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public static void testAppendToUsing(String value) {
        StringBuilder sb = new StringBuilder();
        StringTestUtils.testAppendToUsing("", value, sb);
        StringTestUtils.testAppendToUsing(TEST_STRING, value, sb);
    }

    public static void testInvalidLengthStringsUsing(String value) {
        byte[] bytes = new byte[Short.MAX_VALUE + 1];
        Random random = ThreadLocalRandom.current();
        random.nextBytes(bytes);
        java.lang.String str = new java.lang.String(bytes);
        StringTestUtils.testInvalidLengthUsing(bytes, value);
        StringTestUtils.testInvalidLengthUsing(str, value);
    }

    public static void testInvalidLengthReadUsing(String value) {
        ByteBuf buffer = Unpooled.buffer();
        byte[] bytes = new byte[Short.MAX_VALUE + 1];
        Random random = ThreadLocalRandom.current();
        random.nextBytes(bytes);
        buffer.writeShort(bytes.length);
        buffer.writeBytes(bytes);
        try {
            value.readFrom(buffer);
            fail("Expected a parse exception");
        } catch (ParseException ex) {
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    public static void testInvalidLengthReadUsing(CompactString value) {
        ByteBuf buffer = Unpooled.buffer();
        byte[] bytes = new byte[Short.MAX_VALUE + 1];
        Random random = ThreadLocalRandom.current();
        random.nextBytes(bytes);
        ByteUtils.writeUnsignedVarInt(bytes.length + 1, buffer);
        buffer.writeBytes(bytes);
        try {
            value.readFrom(buffer);
            fail("Expected a parse exception");
        } catch (ParseException ex) {
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }
}