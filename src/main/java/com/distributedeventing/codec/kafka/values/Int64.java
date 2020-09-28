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

public final class Int64 implements Value {

    public static final Int64Factory FACTORY = new Int64Factory();

    private long value;

    public Int64() {
        this(0);
    }

    public Int64(final long value) {
        this.value = value;
    }

    @Override
    public void writeTo(final ByteBuf buffer) {
        buffer.writeLong(value);
    }

    @Override
    public void readFrom(final ByteBuf buffer) {
        value = buffer.readLong();
    }

    @Override
    public int sizeInBytes() {
        return 8;
    }

    @Override
    public void reset() {
        value = 0;
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        return sb.append(value);
    }

    public long value() {
        return value;
    }

    public void value(final long value) {
        this.value = value;
    }

    public static final class Int64Factory implements ValueFactory<Int64> {

        @Override
        public Int64 createInstance() {
            return new Int64();
        }

        private Int64Factory() {}
    }
}