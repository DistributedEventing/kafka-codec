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

import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.exceptions.ParseException;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

public abstract class CompositeValue implements Value {

    protected Schema schema;

    protected Schema.FieldsIterator fieldsIterator;

    protected final Map<Field, Value> fieldValueBindings = new HashMap<Field, Value>();

    @Override
    public void writeTo(final ByteBuf buffer) {
        fieldsIterator.reset();
        while (fieldsIterator.hasNext()) {
            fieldValueBindings.get(fieldsIterator.next()).writeTo(buffer);
        }
    }

    @Override
    public void readFrom(final ByteBuf buffer) throws ParseException {
        fieldsIterator.reset();
        while (fieldsIterator.hasNext()) {
            fieldValueBindings.get(fieldsIterator.next()).readFrom(buffer);
        }
    }

    @Override
    public int sizeInBytes() {
        int size = 0;
        fieldsIterator.reset();
        while (fieldsIterator.hasNext()) {
            size += fieldValueBindings.get(fieldsIterator.next()).sizeInBytes();
        }
        return size;
    }

    @Override
    public void reset() {
        fieldsIterator.reset();
        while (fieldsIterator.hasNext()) {
            fieldValueBindings.get(fieldsIterator.next()).reset();
        }
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        fieldsIterator.reset();
        while (fieldsIterator.hasNext()) {
            final Field field = fieldsIterator.next();
            sb.append("{");
            field.appendTo(sb);
            sb.append("=");
            fieldValueBindings.get(field).appendTo(sb);
            sb.append("}");
        }
        return sb;
    }

    protected CompositeValue(final Schema schema) {
        this.schema = schema;
        this.fieldsIterator = schema.getIterator();
    }
}