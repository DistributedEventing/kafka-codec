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
package com.distributedeventing.codec.kafka;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

public class ByteUtils {

    public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    public static void writeUnsignedInt(final long value,
                                        final int offset,
                                        final ByteBuf buffer) {
        buffer.setInt(offset, (int) (value & 0xffffffffL));
    }

    public static void writeUnsignedInt(final long value,
                                        final ByteBuf buffer) {
        buffer.writeInt((int) (value & 0xffffffffL));
    }

    public static long readUnsignedInt(final ByteBuf buffer) {
        return (buffer.readInt() & 0xffffffffL);
    }

    public static void writeVarInt(final int value,
                                   final ByteBuf buffer) {
        writeUnsignedVarInt((value << 1) ^ (value >> 31), buffer);
    }

    public static void writeUnsignedVarInt(final int value,
                                           final ByteBuf buffer) {
        int v = value;
        while ((v & 0xffffff80) != 0L) {
            byte b = (byte) ((v & 0x7f) | 0x80);
            buffer.writeByte(b);
            v >>>= 7;
        }
        buffer.writeByte((byte) v);
    }

    public static int readVarInt(final ByteBuf buffer) {
        int value = readUnsignedVarInt(buffer);
        return (value >>> 1) ^ -(value & 1);
    }

    public static int readUnsignedVarInt(final ByteBuf buffer) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buffer.readByte()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 28) {
                throw new InvalidVarIntException(value);
            }
        }
        value |= b << i;
        return value;
    }

    public static int sizeOfVarInt(final int value) {
        return sizeOfUnsignedVarInt((value << 1) ^ (value >> 31));
    }

    public static int sizeOfUnsignedVarInt(final int value) {
        int bytes = 1;
        int v = value;
        while ((v & 0xffffff80) != 0L) {
            bytes += 1;
            v >>>= 7;
        }
        return bytes;
    }

    public static void writeVarlong(final long value,
                                    final ByteBuf buffer) {
        long v = (value << 1) ^ (value >> 63);
        while ((v & 0xffffffffffffff80L) != 0L) {
            byte b = (byte) ((v & 0x7f) | 0x80);
            buffer.writeByte(b);
            v >>>= 7;
        }
        buffer.writeByte((byte) v);
    }

    public static long readVarlong(final ByteBuf buffer)  {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = buffer.readByte()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
            if (i > 63) {
                throw new InvalidVarLongException(value);
            }
        }
        value |= b << i;
        return (value >>> 1) ^ -(value & 1);
    }

    public static int sizeOfVarLong(final long value) {
        long v = (value << 1) ^ (value >> 63);
        int bytes = 1;
        while ((v & 0xffffffffffffff80L) != 0L) {
            bytes += 1;
            v >>>= 7;
        }
        return bytes;
    }

    public static boolean equal(final byte[] bytes1,
                                final byte[] bytes2) {
        if (bytes1 == null && bytes2 != null) {
            return false;
        }
        if (bytes2 == null && bytes1 != null) {
            return false;
        }
        if (bytes1.length != bytes2.length) {
            return false;
        }
        for (int i=0; i<bytes1.length; ++i) {
            if (bytes1[i] != bytes2[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equal(final byte[] bytes1,
                                final int bytes1Offset,
                                final int bytes1Length,
                                final byte[] bytes2,
                                final int bytes2Offset,
                                final int bytes2Length) {
        if (bytes1 == null && bytes2 != null) {
            return false;
        }
        if (bytes2 == null && bytes1 != null) {
            return false;
        }
        if (bytes1Length != bytes2Length) {
            return false;
        }
        for (int i=0; i<bytes1Length; ++i) {
            if (bytes1[bytes1Offset + i] != bytes2[bytes2Offset + i]) {
                return false;
            }
        }
        return true;
    }

    public static class InvalidVarIntException extends IllegalArgumentException {

        public InvalidVarIntException (final int value) {
            super (String.format("Invalid varint, most significant bit of the 5th byte is set: %s",
                                 Integer.toHexString(value)));
        }
    }

    public static class InvalidVarLongException extends IllegalArgumentException {

        public InvalidVarLongException (final long value) {
            super (String.format("Invalid varlong, most significant bit of the 10th byte is set: %s",
                                 Long.toHexString(value)));
        }
    }
}