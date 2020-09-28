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

import com.distributedeventing.codec.kafka.ByteUtils;
import io.netty.buffer.ByteBuf;

public final class UnsignedInt32 implements Value {

    public static final UnsignedInt32Factory FACTORY = new UnsignedInt32Factory();

    private long value;

    public UnsignedInt32() {
        this(0);
    }

    public UnsignedInt32(final long value) {
        this.value = value;
    }

    @Override
    public void writeTo(final ByteBuf buffer) {
        ByteUtils.writeUnsignedInt(value, buffer);
    }

    @Override
    public void readFrom(final ByteBuf buffer) {
        value = ByteUtils.readUnsignedInt(buffer);
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

    public long value() {
        return value;
    }

    public void value(final long value) {
        this.value = value;
    }

    public static final class UnsignedInt32Factory implements ValueFactory<UnsignedInt32> {

        @Override
        public UnsignedInt32 createInstance() {
            return new UnsignedInt32();
        }

        private UnsignedInt32Factory() {}
    }
}