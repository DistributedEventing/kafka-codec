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

public final class Int8 implements Value {

    public static final Int8Factory FACTORY = new Int8Factory();

    private byte value;

    public Int8() {
        this((byte) 0);
    }

    public Int8(final byte value) {
        this.value = value;
    }

    @Override
    public void writeTo(final ByteBuf buffer) {
        buffer.writeByte(value);
    }

    @Override
    public void readFrom(final ByteBuf buffer) {
        value = buffer.readByte();
    }

    @Override
    public int sizeInBytes() {
        return 1;
    }

    @Override
    public void reset() {
        this.value = 0;
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        return sb.append(value);
    }

    public byte value() {
        return value;
    }

    public void value(final byte value) {
        this.value = value;
    }

    public static final class Int8Factory implements ValueFactory<Int8> {

        @Override
        public Int8 createInstance() {
            return new Int8();
        }

        private Int8Factory() {}
    }
}