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

public final class VarInt implements Value {

    public static final VarIntFactory FACTORY = new VarIntFactory();

    private int value;

    public VarInt() {
        this(0);
    }

    public VarInt(final int value) {
        this.value = value;
    }

    @Override
    public void writeTo(final ByteBuf buffer) {
        ByteUtils.writeVarInt(value, buffer);
    }

    @Override
    public void readFrom(final ByteBuf buffer) throws ParseException {
        value = ByteUtils.readVarInt(buffer);
    }

    @Override
    public int sizeInBytes() {
        return ByteUtils.sizeOfVarInt(value);
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

    public static final class VarIntFactory implements ValueFactory<VarInt> {

        @Override
        public VarInt createInstance() {
            return new VarInt();
        }

        private VarIntFactory() {}
    }
}