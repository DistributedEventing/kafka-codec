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

public class BytesTestUtils {

    public static final int VL_SIZEOF_LENGTH_0 = ByteUtils.sizeOfVarInt(0);

    static final byte[] FEW_BYTES = new byte[] {Byte.MIN_VALUE,
                                                (byte) 0,
                                                Byte.MAX_VALUE};

    public static void testWriteToUsing(Bytes value) {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            BytesTestUtils.testWriteToUsing(value, random);
        }
        // test length = 0
        BytesTestUtils.testWriteToUsing(value, new byte[] {});
    }

    private static void testWriteToUsing(Bytes value,
                                         Random random) {
        int length = random.nextInt(Short.MAX_VALUE);
        byte[] bytesToWrite = new byte[length];
        random.nextBytes(bytesToWrite);
        testWriteToUsing(value, bytesToWrite);
    }

    private static void testWriteToUsing(Bytes value,
                                         byte[] bytesToWrite) {
        value.value(bytesToWrite);
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        int length = bytesToWrite.length;
        int lenOfLen = LengthUtils.getLenOfLen(value, length);
        assertEquals(lenOfLen + length, buffer.readableBytes());
        int lengthRead = LengthUtils.readLengthFrom(buffer, value);
        assertEquals(length, lengthRead);
        byte[] bytesRead = new byte[length];
        buffer.readBytes(bytesRead);
        assertArrayEquals(bytesToWrite, bytesRead);
    }

    public static void testWriteOfNullUsing(Bytes value) {
        NullableValue nullableValue = (NullableValue) value;
        nullableValue.nullify();
        ByteBuf buffer = Unpooled.buffer();
        value.writeTo(buffer);
        assertEquals(LengthUtils.getSizeInBytesOfNull(value), buffer.readableBytes());
        assertEquals(-1, LengthUtils.readLengthFrom(buffer, value));
    }

    public static void testReadFromUsing(Bytes value) {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            BytesTestUtils.testReadFromUsing(value, random);
        }
        // test length = 0
        BytesTestUtils.testReadFromUsing(value, new byte[] {});
    }

    public static void testReadFromUsing(Bytes value,
                                         Random random) {
        int length = random.nextInt(Short.MAX_VALUE);
        byte[] bytesToWrite = new byte[length];
        random.nextBytes(bytesToWrite);
        testReadFromUsing(value, bytesToWrite);
    }

    public static void testReadFromUsing(Bytes value,
                                         byte[] bytesToWrite) {
        ByteBuf buffer = Unpooled.buffer();
        int length = bytesToWrite.length;
        LengthUtils.writeLengthTo(value, length, buffer);
        buffer.writeBytes(bytesToWrite);
        try {
            value.readFrom(buffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        assertEquals(length, value.length());
        byte[] bytesRead = new byte[value.length()];
        System.arraycopy(value.value(), 0, bytesRead, 0, value.length());
        assertArrayEquals(bytesToWrite, bytesRead);
    }

    public static void testReadOfNullUsing(Bytes value) {
        ByteBuf buffer = Unpooled.buffer();
        LengthUtils.writeLengthTo(value, -1, buffer);
        try {
            value.readFrom(buffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        assertTrue(((NullableValue) value).isNull());
        assertEquals(-1, value.length());
    }

    public static void testSizeInBytesUsing(Bytes value) {
        assertEquals(LengthUtils.getSizeInBytesOfEmpty(value), value.sizeInBytes());
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            BytesTestUtils.testSizeInBytesUsing(value, random);
        }
        if (value instanceof NullableValue) {
            NullableValue nullableValue = (NullableValue) value;
            nullableValue.nullify();
            assertEquals(LengthUtils.getSizeInBytesOfNull(value), value.sizeInBytes());
            nullableValue.empty();
            assertEquals(LengthUtils.getSizeInBytesOfEmpty(value), value.sizeInBytes());
        }
    }

    public static void testSizeInBytesUsing(Bytes value,
                                            Random random) {
        int length = random.nextInt(Short.MAX_VALUE);
        byte[] bytesToWrite = new byte[length];
        random.nextBytes(bytesToWrite);
        testSizeInBytesUsing(value, bytesToWrite);
    }

    public static void testSizeInBytesUsing(Bytes value,
                                            byte[] bytesToWrite) {
        value.value(bytesToWrite);
        int length = bytesToWrite.length;
        int lenOfLen = LengthUtils.getLenOfLen(value, length);
        assertEquals(lenOfLen + length, value.sizeInBytes());
    }

    public static void testAccessorsAndMutatorsUsing(Bytes value) {
        value.value(null);
        assertEquals(LengthUtils.getLenOfNull(value), value.length());
        assertEquals(LengthUtils.getSizeInBytesOfNull(value), value.sizeInBytes());
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            BytesTestUtils.testAccessorsAndMutatorsUsing(value, random);
        }
        value.value(new byte[0]);
        assertEquals(0, value.length());
        assertEquals(LengthUtils.getLenOfLen(value, 0), value.sizeInBytes());

        value.value(FEW_BYTES, 0, FEW_BYTES.length);
        assertEquals(FEW_BYTES.length, value.length());
        byte[] bytesRead = new byte[value.length()];
        System.arraycopy(value.value(), 0, bytesRead, 0, value.length());
        assertArrayEquals(FEW_BYTES, bytesRead);

        value.value(null, 0, 0);
        assertEquals(LengthUtils.getLenOfNull(value), value.length());
        assertEquals(LengthUtils.getSizeInBytesOfNull(value), value.sizeInBytes());
    }

    public static void testAccessorsAndMutatorsUsing(Bytes value,
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

    public static void testResetUsing(Bytes value) {
        value.value(FEW_BYTES);
        int length = FEW_BYTES.length;
        int lenOfLen = LengthUtils.getLenOfLen(value, length);
        assertEquals(length, value.length());
        assertEquals(lenOfLen + length, value.sizeInBytes());
        value.reset();
        length = 0;
        lenOfLen = LengthUtils.getLenOfLen(value, length);
        assertEquals(length, value.length());
        assertEquals(lenOfLen + length, value.sizeInBytes());
    }

    public static void testNegativeLengthUsing(Bytes value) throws ParseException {
        ByteBuf buffer = Unpooled.buffer();
        LengthUtils.writeLengthTo(value, -1, buffer);
        value.readFrom(buffer);
    }

    public static void testAppendToUsing(Bytes bytes) {
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<100; ++i) {
            int length = random.nextInt(Short.MAX_VALUE);
            byte[] bytesToWrite = new byte[length];
            random.nextBytes(bytesToWrite);
            _testAppendToUsing(bytes, bytesToWrite);
        }

        // test length = 0
        _testAppendToUsing(bytes, new byte[] {});

        if (bytes instanceof NullableValue) {
            ((NullableValue) bytes).nullify();
            StringBuilder sb = new StringBuilder();
            bytes.appendTo(sb);
            assertEquals("null", sb.toString());
        }
    }

    private static void _testAppendToUsing(Bytes bytes,
                                           byte[] bytesToWrite) {
        bytes.value(bytesToWrite);
        _testAppendToUsing(bytes);
    }

    private static void _testAppendToUsing(Bytes bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append(bytes.getClass().getSimpleName())
          .append("(length=")
          .append(bytes.length())
          .append(")");
        java.lang.String expectedValue = sb.toString();
        sb.setLength(0);
        bytes.appendTo(sb);
        assertEquals(expectedValue, sb.toString());
    }

    private static final class LengthUtils {

        static int getLenOfLen(Bytes value, int length) {
            if (value instanceof CompactBytes) {
                return ByteUtils.sizeOfUnsignedVarInt(length + 1);
            } else if (value  instanceof VlBytes) {
                return ByteUtils.sizeOfVarInt(length);
            } else if (value instanceof UvilBytes) {
                return ByteUtils.sizeOfUnsignedVarInt(length);
            } else {
                return 4;
            }
        }

        static int readLengthFrom(ByteBuf buffer,
                                  Bytes value) {
            if (value instanceof CompactBytes) {
                return ByteUtils.readUnsignedVarInt(buffer) - 1;
            } else if (value instanceof VlBytes) {
                return ByteUtils.readVarInt(buffer);
            } else if (value instanceof UvilBytes) {
                return ByteUtils.readUnsignedVarInt(buffer);
            } else {
                return buffer.readInt();
            }
        }

        static void writeLengthTo(Bytes value,
                                  int length,
                                  ByteBuf buffer) {
            if (value instanceof CompactBytes) {
                ByteUtils.writeUnsignedVarInt(length + 1, buffer);
            } else if (value instanceof VlBytes) {
                ByteUtils.writeVarInt(length, buffer);
            } else if (value instanceof UvilBytes) {
                ByteUtils.writeUnsignedVarInt(length, buffer);
            } else {
                buffer.writeInt(length);
            }
        }

        static int getLenOfNull(Bytes value) {
            if (value instanceof NullableValue) {
                return -1;
            } else {
                return 0;
            }
        }

        static int getSizeInBytesOfNull(Bytes value) {
            return getLenOfLen(value, getLenOfNull(value));
        }

        static int getSizeInBytesOfEmpty(Bytes value) {
            return getLenOfLen(value, 0);
        }
    }
}