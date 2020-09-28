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

public class NullableArray<T extends Value> extends Array<T> implements NullableValue {

    protected boolean isNull;

    public NullableArray(final ValueFactory<T> elementFactory) {
        super(elementFactory);
    }

    @Override
    public boolean isNull() {
        return isNull;
    }

    @Override
    public void nullify() {
        elements.clear();
        isNull = true;
    }

    @Override
    public void empty() {
        elements.clear();
        isNull = false;
    }

    @Override
    public void reset() {
        elements.clear();
        isNull = false;
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        if (isNull) {
            sb.append("null");
            return sb;
        }
        return super.appendTo(sb);
    }

    @Override
    public void value(final T...values) {
        if (values == null) {
            elements.clear();
            isNull = true;
            return;
        }
        isNull = false;
        super.value(values);
    }

    @Override
    public int length() {
        if (isNull) {
            return -1;
        }
        return super.length();
    }

    @Override
    protected void writeLengthTo(final ByteBuf buffer) {
        if (isNull) {
            buffer.writeInt(-1);
            return;
        }
        super.writeLengthTo(buffer);
    }

    @Override
    protected void validateReadLength(final int length) {}

    @Override
    protected void readDataFrom(final ByteBuf buffer,
                                final int length) throws ParseException {
        if (length < 0) {
            elements.clear();
            isNull = true;
            return;
        }
        super.readDataFrom(buffer, length);
    }
}