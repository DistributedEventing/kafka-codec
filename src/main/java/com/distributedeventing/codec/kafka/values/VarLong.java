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

public final class VarLong implements Value {

    public static final VarLongFactory FACTORY = new VarLongFactory();

    private long value;

    public VarLong() {
        this(0);
    }

    public VarLong(final long value) {
        this.value = value;
    }

    @Override
    public void writeTo(final ByteBuf buffer) {
        ByteUtils.writeVarlong(value, buffer);
    }

    @Override
    public void readFrom(final ByteBuf buffer) throws ParseException {
        value = ByteUtils.readVarlong(buffer);
    }

    @Override
    public int sizeInBytes() {
        return ByteUtils.sizeOfVarLong(value);
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

    public static final class VarLongFactory implements ValueFactory<VarLong> {

        @Override
        public VarLong createInstance() {
            return new VarLong();
        }

        private VarLongFactory() {}
    }
}