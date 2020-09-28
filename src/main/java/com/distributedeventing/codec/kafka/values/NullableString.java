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

public class NullableString extends String implements NullableValue {

    public NullableString() {
        this(128, Scaler.DEFAULT_SCALER);
    }

    public NullableString(final int initialCapacity,
                          final Scaler scaler) {
        super(initialCapacity,
              scaler);
    }

    @Override
    public boolean isNull() {
        return length < 0;
    }

    @Override
    public void nullify() {
        length = -1;
    }

    @Override
    public void empty() {
        length = 0;
    }

    @Override
    public java.lang.String valueAsString() {
        if (isNull()) {
            return null;
        }
        return super.valueAsString();
    }

    @Override
    public void value(final byte[] bytes,
                      final int offset,
                      final int length) {
        if (bytes == null) {
            this.length = -1;
            return;
        }
        super.value(bytes, offset, length);
    }

    @Override
    public void value(final byte[] bytes) {
        if (bytes == null) {
            this.length = -1;
            return;
        }
        value(bytes, 0, bytes.length);
    }

    @Override
    public void value(final java.lang.String str) {
        if (str == null) {
            length = -1;
            return;
        }
        super.value(str);
    }

    @Override
    protected void writeLengthTo(final ByteBuf buffer) {
        if (isNull()) {
            buffer.writeShort(-1);
            return;
        }
        super.writeLengthTo(buffer);
    }

    @Override
    protected void writeDataTo(final ByteBuf buffer) {
        if (isNull()) {
            return;
        }
        super.writeDataTo(buffer);
    }

    @Override
    protected void readDataFrom(final ByteBuf buffer,
                                final int length) {
        if (length < 0) {
            return;
        }
        super.readDataFrom(buffer, length);
    }

    @Override
    protected int sizeOfDataInBytes() {
        return Math.max(0, length);
    }

    @Override
    protected void validateReadLength(final int length) throws ParseException {
        if (length > Short.MAX_VALUE) {
            throw new IllegalArgumentException(java.lang.String.format("Length of %s cannot exceed %d, provided length=%d",
                                                                       this.getClass().getName(),
                                                                       Short.MAX_VALUE,
                                                                       length));
        }
    }
}