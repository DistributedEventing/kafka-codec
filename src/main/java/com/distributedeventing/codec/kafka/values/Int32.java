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

public final class Int32 implements Value {

    public static final Int32Factory FACTORY = new Int32Factory();

    private int value;

    public Int32() {
        this(0);
    }

    public Int32(final int value) {
        this.value = value;
    }

    @Override
    public void writeTo(final ByteBuf buffer) {
        buffer.writeInt(value);
    }

    @Override
    public void readFrom(final ByteBuf buffer) {
        value = buffer.readInt();
    }

    @Override
    public int sizeInBytes() {
        return 4;
    }

    @Override
    public void reset() {
        value = 0;
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        return sb.append(value);
    }

    public int value() {
        return value;
    }

    public void value(final int value) {
        this.value = value;
    }

    @Override
    public boolean equals(final Object other) {
        if (other == this) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (!(other instanceof Int32)) {
            return false;
        }
        final Int32 otherInt = (Int32) other;
        return (otherInt.value == this.value);
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(value);
    }

    public static final class Int32Factory implements ValueFactory<Int32> {

        @Override
        public Int32 createInstance() {
            return new Int32();
        }

        private Int32Factory() {}
    }
}