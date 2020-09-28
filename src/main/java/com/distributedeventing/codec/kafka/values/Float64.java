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

public final class Float64 implements Value {

    public static final Float64Factory FACTORY = new Float64Factory();

    private double value;

    public Float64() {
        this(0.0);
    }

    public Float64(final double value) {
        this.value = value;
    }

    @Override
    public void writeTo(final ByteBuf buffer) {
        buffer.writeDouble(value);
    }

    @Override
    public void readFrom(final ByteBuf buffer) {
        value = buffer.readDouble();
    }

    @Override
    public int sizeInBytes() {
        return 8;
    }

    @Override
    public void reset() {
        value = 0.0;
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        return sb.append(value);
    }

    public double value() {
        return value;
    }

    public void value(final double value) {
        this.value = value;
    }

    public static final class Float64Factory implements ValueFactory<Float64> {

        @Override
        public Float64 createInstance() {
            return new Float64();
        }

        private Float64Factory() {}
    }
}