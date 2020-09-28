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

import static org.junit.Assert.*;

public class ArrayTestUtils {

    public static void testWriteToUsing(Array<Int32> arr,
                                        Random random) {
        int len = random.nextInt(102400);
        int[] elements = new int[len];
        for (int i=0; i<len; ++i) {
            elements[i] = random.nextInt();
        }
        testWriteToUsing(arr, elements);
    }

    public static void testWriteToUsing(Array<Int32> arr,
                                        int[] elements) {
        Int32[] valuesToWrite = new Int32[elements.length];
        for (int i=0; i<elements.length; ++i) {
            valuesToWrite[i] = new Int32(elements[i]);
        }
        arr.value(valuesToWrite);
        ByteBuf buffer = Unpooled.buffer();
        arr.writeTo(buffer);
        int expectedBytesWritten = LengthUtils.getLenOfLen(arr) /** length of array **/ +
                                   4 * elements.length /** elements.length Int32s **/;
        assertEquals(expectedBytesWritten, buffer.readableBytes());
        int readLength = LengthUtils.readLenOf(arr, buffer);
        assertEquals(valuesToWrite.length, readLength);
        Int32[] readValues = new Int32[readLength];
        for (int i=0; i<readLength; ++i) {
            Int32 readValue = new Int32();
            readValue.readFrom(buffer);
            readValues[i] = readValue;
        }
        assertArrayEquals(valuesToWrite, readValues);
    }

    public static void testReadFromUsing(Array<Int32> arr,
                                         Random random) {
        int len = random.nextInt(102400);
        int[] elements = new int[len];
        for (int i=0; i<len; ++i) {
            elements[i] = random.nextInt();
        }
        testReadFromUsing(arr, elements);
    }

    public static void testReadFromUsing(Array<Int32> arr,
                                         int[] elements) {
        ByteBuf buffer = Unpooled.buffer();
        LengthUtils.writeLenTo(arr, elements.length, buffer);
        for (int element: elements) {
            buffer.writeInt(element);
        }
        try {
            arr.readFrom(buffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        assertEquals(elements.length, arr.length());
        Array<Int32>.ElementIterator iterator = arr.iterator();
        int i=0;
        while (iterator.hasNext()) {
            Int32 readElement = iterator.next();
            assertEquals(elements[i++], readElement.value());
        }
        assertEquals(i, elements.length);
    }

    public static void testSizeOfInBytesUsing(Array<Int32> arr,
                                              Random random) {
        int len = random.nextInt(102400);
        int[] elements = new int[len];
        for (int i=0; i<len; ++i) {
            elements[i] = random.nextInt();
        }
        testSizeOfInBytesUsing(arr, elements);
    }

    public static void testSizeOfInBytesUsing(Array<Int32> arr,
                                              int[] elements) {
        Int32[] valuesToSet = new Int32[elements.length];
        for (int i=0; i<elements.length; ++i) {
            valuesToSet[i] = new Int32(elements[i]);
        }
        arr.value(valuesToSet);
        int expectedSize = LengthUtils.getLenOfLen(arr) /** for the length of the array **/ +
                           4 * elements.length /** for the elements themselves **/;
        assertEquals(expectedSize, arr.sizeInBytes());
    }

    public static void testAccessorsAndMutatorsUsing(Array<Int32> arr,
                                                     Random random) {
        int len = random.nextInt(102400);
        int[] elements = new int[len];
        for (int i=0; i<len; ++i) {
            elements[i] = random.nextInt();
        }
        testAccessorsAndMutatorsUsing(arr, elements);
    }

    public static void testAccessorsAndMutatorsUsing(Array<Int32> arr,
                                                     int[] elements) {
        Int32[] valuesToSet = new Int32[elements.length];
        for (int i=0; i<elements.length; ++i) {
            valuesToSet[i] = new Int32(elements[i]);
        }
        arr.value(valuesToSet);
        assertEquals(elements.length, arr.length());
        Array<Int32>.ElementIterator iterator = arr.iterator();
        int i=0;
        while (iterator.hasNext()) {
            Int32 gottenValue = iterator.next();
            assertEquals(valuesToSet[i++], gottenValue);
        }
        assertEquals(elements.length, i);
    }

    public static void testInvalidReadLengthUsing(int lengthToUse,
                                                  Array<Int32> arr) {
        ByteBuf buffer = Unpooled.buffer();
        LengthUtils.writeLenTo(arr, lengthToUse, buffer);
        try {
            arr.readFrom(buffer);
            fail("Expected to see a ParseException");
        } catch (ParseException e) {
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    public static void testAppendToUsing(Array<Int32> arr,
                                         Random random) {
        int len = random.nextInt(102400);
        int[] elements = new int[len];
        for (int i=0; i<len; ++i) {
            elements[i] = random.nextInt();
        }
        testAppendToUsing(arr, elements);
    }

    public static void testAppendToUsing(Array<Int32> arr,
                                         int[] elements) {
        StringBuilder sb = new StringBuilder("[");
        Int32[] values = new Int32[elements.length];
        for (int i=0; i<elements.length; ++i) {
            sb.append(elements[i]);
            if (i != elements.length - 1) {
                sb.append(", ");
            }
            values[i] = new Int32(elements[i]);
        }
        sb.append("]");
        java.lang.String expectedResult = sb.toString();
        arr.value(values);
        sb.setLength(0);
        arr.appendTo(sb);
        java.lang.String result = sb.toString();
        assertEquals(expectedResult, result);
    }

    public static void testResetUsing(Array<Boolean> arr) {
        _testResetUsing(arr);
    }

    public static void testResetUsing(NullableArray<Boolean> arr) {
        _testResetUsing(arr);
        assertFalse(arr.isNull());
    }

    public static void testResetUsing(CompactNullableArray<Boolean> arr) {
        _testResetUsing(arr);
        assertFalse(arr.isNull());
    }

    private static void _testResetUsing(Array<Boolean> arr) {
        arr.add(new Boolean());
        Array<Boolean>.ElementIterator iterator = arr.iterator();
        assertTrue(iterator.hasNext());
        assertFalse(iterator.next().value());
        assertEquals(1, arr.length());
        arr.reset();
        assertEquals(0, arr.length());
        iterator.reset();
        assertFalse(iterator.hasNext());
    }

    public static void testAdditionToEmptyArrayUsing(Random random,
                                                     Array<Int32> arr) {
        Array<Int32>.ElementIterator iterator = arr.iterator();
        assertFalse(iterator.hasNext());
        int i = random.nextInt();
        Int32 value = new Int32(i);
        arr.add(value);
        assertEquals(1, arr.length());
        assertTrue(iterator.hasNext());
        assertEquals(value, iterator.next());
    }

    public static void testWriteOfNullUsing(NullableArray<Int32> arr) {
        arr.nullify();
        assertTrue(arr.isNull());
        _testWriteOfNullUsing(arr);
    }

    public static void testWriteOfNullUsing(CompactNullableArray<Int32> arr) {
        arr.nullify();
        assertTrue(arr.isNull());
        _testWriteOfNullUsing(arr);
    }

    private static void _testWriteOfNullUsing(Array<Int32> arr) {
        assertEquals(-1, arr.length());
        ByteBuf buffer = Unpooled.buffer();
        arr.writeTo(buffer);
        assertEquals(LengthUtils.getLenOfLen(arr), buffer.readableBytes());
        assertEquals(-1, LengthUtils.readLenOf(arr, buffer));
    }

    public static void testReadOfNullUsing(NullableArray<Int32> arr) {
        _testReadOfNullUsing(arr);
        assertTrue(arr.isNull());
    }

    public static void testReadOfNullUsing(CompactNullableArray<Int32> arr) {
        _testReadOfNullUsing(arr);
        assertTrue(arr.isNull());
    }

    private static void _testReadOfNullUsing(Array<Int32> arr) {
        ByteBuf buffer = Unpooled.buffer();
        LengthUtils.writeLenTo(arr, -1, buffer);
        try {
            arr.readFrom(buffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        assertEquals(-1, arr.length());
    }

    public static void testSizeOfNullableInBytesUsing(Array<Int32> arr) {
        assertEquals(LengthUtils.getLengthOfNullArray(arr), arr.sizeInBytes());
    }

    public static void testAppendToOfNullArrayUsing(Array<Int32> arr) {
        StringBuilder sb = new StringBuilder();
        arr.appendTo(sb);
        assertEquals("null", sb.toString());
    }

    public static final class LengthUtils {

        public static final int LENGTH_OF_NULL_ARRAY = ByteUtils.sizeOfUnsignedVarInt(0);

        public static int getLenOfLen(Array<Int32> arr) {
            if (arr instanceof CompactArray) {
                return ByteUtils.sizeOfUnsignedVarInt(arr.length() + 1);
            } else if (arr instanceof VlArray) {
                return ByteUtils.sizeOfVarInt(arr.length());
            } else if (arr instanceof UvilArray) {
                return ByteUtils.sizeOfUnsignedVarInt(arr.length());
            } else {
                return 4;
            }
        }

        public static void writeLenTo(Array<Int32> arr,
                                      int length,
                                      ByteBuf buffer) {
            if (arr instanceof CompactArray) {
                ByteUtils.writeUnsignedVarInt(length + 1, buffer);
            } else if (arr instanceof VlArray) {
                ByteUtils.writeVarInt(length, buffer);
            } else if (arr instanceof UvilArray) {
                ByteUtils.writeUnsignedVarInt(length, buffer);
            } else {
                buffer.writeInt(length);
            }
        }

        public static int readLenOf(Array<Int32> arr,
                                    ByteBuf buffer) {
            if (arr instanceof CompactArray) {
                return ByteUtils.readUnsignedVarInt(buffer) - 1;
            } else if (arr instanceof VlArray) {
                return ByteUtils.readVarInt(buffer);
            } else if (arr instanceof UvilArray) {
                return ByteUtils.readUnsignedVarInt(buffer);
            } else {
                return buffer.readInt();
            }
        }

        public static int getLengthOfNullArray(Array<Int32> arr) {
            if (arr instanceof CompactNullableArray) {
                return LENGTH_OF_NULL_ARRAY;
            } else {
                return 4;
            }
        }
    }
}