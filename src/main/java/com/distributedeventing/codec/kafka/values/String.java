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
import com.distributedeventing.codec.kafka.ByteUtils;
import com.distributedeventing.codec.kafka.Scaler;
import io.netty.util.internal.StringUtil;

public class String implements Value {

    protected byte[] bytes;

    protected short length;

    protected final Scaler scaler;

    public String() {
        this(128, Scaler.DEFAULT_SCALER);
    }

    public String(final int initialCapacity,
                  final Scaler scaler) {
        if (initialCapacity < 1) {
            throw new IllegalArgumentException(java.lang.String.format("Initial capacity of %s cannot be less than 1",
                                                                       this.getClass().getName()));
        } else if (initialCapacity > Short.MAX_VALUE) {
            throw new IllegalArgumentException(java.lang.String.format("Initial capacity of %s cannot exceed %d",
                                                                       this.getClass().getName(),
                                                                       Short.MAX_VALUE));
        }
        if (scaler == null) {
            throw new IllegalArgumentException("Scaler cannot be null");
        }
        bytes = new byte[initialCapacity];
        this.scaler = scaler;
    }

    @Override
    public void writeTo(final ByteBuf buffer) {
        writeLengthTo(buffer);
        writeDataTo(buffer);
    }

    @Override
    public void readFrom(final ByteBuf buffer) throws ParseException {
        final int len = readLengthFrom(buffer);
        validateReadLength(len);
        ensureCapacity(len);
        length = (short) len;
        readDataFrom(buffer, length);
    }

    @Override
    public int sizeInBytes() {
        return sizeOfLengthInBytes() + sizeOfDataInBytes();
    }

    @Override
    public void reset() {
        length = 0;
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        return sb.append(valueAsString());
    }

    public int length() {
        return length;
    }

    public byte[] value() {
        return bytes;
    }

    public java.lang.String valueAsString() {
       return new java.lang.String(bytes, 0, length, ByteUtils.CHARSET_UTF8);
    }

    public void value(final byte[] bytes) {
        if (bytes == null) {
            this.length = 0;
            return;
        }
        value(bytes, 0, bytes.length);
    }

    public void value(final byte[] bytes,
                      final int offset,
                      final int length) {
        if (bytes == null) {
            this.length = 0;
            return;
        }
        if (length > Short.MAX_VALUE) {
            throw new IllegalArgumentException(java.lang.String.format("Length of %s cannot exceed %d, provided length=%d",
                                                                       this.getClass().getName(),
                                                                       Short.MAX_VALUE,
                                                                       length));
        }
        if (length > this.bytes.length) {
            ensureCapacity(length);
        }
        System.arraycopy(bytes, offset, this.bytes, 0, length);
        this.length = (short) length;
    }

    public void value(final java.lang.String str) {
        if (StringUtil.isNullOrEmpty(str)) {
            length = 0;
            return;
        }
        final byte[] strBytes = str.getBytes(ByteUtils.CHARSET_UTF8);
        value(strBytes, 0, strBytes.length);
    }

    protected void writeLengthTo(final ByteBuf buffer) {
        buffer.writeShort(length);
    }

    protected void writeDataTo(final ByteBuf buffer) {
        buffer.writeBytes(bytes, 0, length);
    }

    protected int readLengthFrom(final ByteBuf buffer) {
        return buffer.readShort();
    }

    protected void validateReadLength(final int length) throws ParseException {
        if (length < 0) {
            throw new ParseException(java.lang.String.format("Length of %s cannot be -ve; length read = %d",
                                                             this.getClass().getName(),
                                                             length));
        } else if (length > Short.MAX_VALUE) {
            throw new ParseException(java.lang.String.format("Length of %s cannot exceed %d, length read=%d",
                                                              this.getClass().getName(),
                                                              Short.MAX_VALUE,
                                                              length));
        }
    }

    protected void ensureCapacity(final int expectedMinCapacity) {
        final int currentCapacity = bytes.length;
        if (expectedMinCapacity <= currentCapacity) {
            return;
        }
        int newCapacity = currentCapacity;
        while (newCapacity < expectedMinCapacity) {
            newCapacity = scaler.scale(newCapacity);
        }
        final byte[] newBytes = new byte[newCapacity];
        System.arraycopy(bytes, 0, newBytes, 0, currentCapacity);
        bytes = newBytes;
    }

    protected void readDataFrom(final ByteBuf buffer,
                                final int length) {
        buffer.readBytes(bytes, 0, length);
    }

    protected int sizeOfLengthInBytes() {
        return 2;
    }

    protected int sizeOfDataInBytes() {
        return length;
    }
}