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

public class UvilArray<T extends Value> extends Array<T> {

    public UvilArray(final ValueFactory<T> elementFactory) {
        super(elementFactory);
    }

    @Override
    protected void writeLengthTo(final ByteBuf buffer) {
        ByteUtils.writeUnsignedVarInt(elements.size(), buffer);
    }

    @Override
    protected int readLengthFrom(final ByteBuf buffer) {
        return ByteUtils.readUnsignedVarInt(buffer);
    }

    @Override
    protected int sizeOfLenInBytes() {
        return ByteUtils.sizeOfUnsignedVarInt(elements.size());
    }
}