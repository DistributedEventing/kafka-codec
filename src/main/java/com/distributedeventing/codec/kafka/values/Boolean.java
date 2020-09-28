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

public final class Boolean implements Value {

    public static final BooleanFactory FACTORY = new BooleanFactory();

    private boolean value;

    public Boolean() {
        this(false);
    }

    public Boolean(final boolean value) {
        this.value = value;
    }

    @Override
    public void writeTo(final ByteBuf buffer) {
        buffer.writeByte(value? 1: 0);
    }

    @Override
    public void readFrom(final ByteBuf buffer) {
        value = (buffer.readByte() == 0? false: true);
    }

    @Override
    public int sizeInBytes() {
        return 1;
    }

    @Override
    public void reset() {
        value = false;
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        return sb.append(value);
    }

    public boolean value() {
        return value;
    }

    public void value(final boolean value) {
        this.value = value;
    }

    public static final class BooleanFactory implements ValueFactory<Boolean> {

        @Override
        public Boolean createInstance() {
            return new Boolean();
        }

        private BooleanFactory() {}
    }
}