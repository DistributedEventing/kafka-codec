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
import com.distributedeventing.codec.kafka.Scaler;

public class CompactString extends String {

    public CompactString() {
        this(128, Scaler.DEFAULT_SCALER);
    }

    public CompactString(final int initialCapacity,
                         final Scaler scaler) {
        super(initialCapacity,
              scaler);
    }

    @Override
    protected void writeLengthTo(final ByteBuf buffer) {
        ByteUtils.writeUnsignedVarInt(length + 1, buffer);
    }

    @Override
    protected int readLengthFrom(final ByteBuf buffer) {
        return ByteUtils.readUnsignedVarInt(buffer) - 1;
    }

    @Override
    protected int sizeOfLengthInBytes() {
        return ByteUtils.sizeOfUnsignedVarInt(length + 1);
    }

    public static final class CompactStringFactory implements ValueFactory<CompactString> {

        private final int intialCapacity;

        private final Scaler scaler;

        public CompactStringFactory() {
            this(128, Scaler.DEFAULT_SCALER);
        }

        public CompactStringFactory(final int intialCapacity,
                                    final Scaler scaler) {
            this.intialCapacity = intialCapacity;
            this.scaler = scaler;
        }

        @Override
        public CompactString createInstance() {
            return new CompactString(intialCapacity,
                                     scaler);
        }
    }
}