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
import com.distributedeventing.codec.kafka.Scaler;

public class Bytes implements Value {

    protected byte[] bytes;

    protected int length;

    protected final Scaler scaler;

    public Bytes() {
        this(128, Scaler.DEFAULT_SCALER);
    }

    public Bytes(final int initialCapacity,
                 final Scaler scaler) {
        if (initialCapacity < 1) {
            throw new IllegalArgumentException(java.lang.String.format("Initial capacity of %s cannot be less than 1",
                                                                       this.getClass().getName()));
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
        length = len;
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
        return sb.append(this.getClass().getSimpleName())
                 .append("(length=")
                 .append(length)
                 .append(")");
    }

    public int length() {
        return length;
    }

    public byte[] value() {
        return bytes;
    }

    public void value(final byte[] bytes) {
        if (bytes == null) {
            this.length = 0;
            return;
        }
        copyFrom(bytes, 0, bytes.length);
    }

    public void value(final byte[] bytes,
                      final int offset,
                      final int length) {
        if (bytes == null) {
            this.length = 0;
            return;
        }
        copyFrom(bytes, offset, length);
    }

    protected void writeLengthTo(final ByteBuf buffer) {
        buffer.writeInt(length);
    }

    protected void writeDataTo(final ByteBuf buffer) {
        buffer.writeBytes(bytes, 0, length);
    }

    protected int readLengthFrom(final ByteBuf buffer) {
        return buffer.readInt();
    }

    protected void validateReadLength(final int length) throws ParseException {
        if (length < 0) {
            throw new ParseException(java.lang.String.format("Length of %s cannot be -ve; length read = %d",
                                                             this.getClass().getName(),
                                                             length));
        }
    }

    protected void ensureCapacity(final int minExpectedCapacity) {
        final int currentCapacity = bytes.length;
        if (minExpectedCapacity <= currentCapacity) {
            return;
        }
        int newCapacity = currentCapacity;
        while (newCapacity < minExpectedCapacity) {
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
        return 4;
    }

    protected int sizeOfDataInBytes() {
        return length;
    }

    private void copyFrom(final byte[] bytes,
                          final int offset,
                          final int length) {
        if (length > this.bytes.length) {
            ensureCapacity(length);
        }
        System.arraycopy(bytes, offset, this.bytes, 0, length);
        this.length = length;
    }
}