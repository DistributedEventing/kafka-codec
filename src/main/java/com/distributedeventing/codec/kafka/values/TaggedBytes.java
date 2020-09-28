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

public class TaggedBytes extends UvilBytes {

    private int tag;

    public TaggedBytes() {
        this(128, Scaler.DEFAULT_SCALER);
    }

    public TaggedBytes(final int initialCapacity,
                       final Scaler scaler) {
        super(initialCapacity,
              scaler);
    }

    @Override
    public void writeTo(final ByteBuf buffer) {
        ByteUtils.writeUnsignedVarInt(tag, buffer);
        super.writeTo(buffer);
    }

    @Override
    public void readFrom(final ByteBuf buffer) throws ParseException {
        tag = ByteUtils.readUnsignedVarInt(buffer);
        super.readFrom(buffer);
    }

    @Override
    public int sizeInBytes() {
        return ByteUtils.sizeOfUnsignedVarInt(tag) + super.sizeInBytes();
    }

    public int tag() {
        return tag;
    }

    public void tag(final int tag) {
        this.tag = tag;
    }

    public static final class TaggedBytesFactory implements ValueFactory<TaggedBytes> {

        private final int initialCapacity;

        private final Scaler scaler;

        public TaggedBytesFactory() {
            this(128,
                 Scaler.DEFAULT_SCALER);
        }

        public TaggedBytesFactory(final int initialCapacity,
                                  final Scaler scaler) {
            this.initialCapacity = initialCapacity;
            this.scaler = scaler;
        }

        @Override
        public TaggedBytes createInstance() {
            return new TaggedBytes(initialCapacity, scaler);
        }
    }
}