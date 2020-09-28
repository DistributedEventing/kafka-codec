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
package com.distributedeventing.codec.kafka.headers;

import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.Value;

import java.util.HashMap;
import java.util.Map;

abstract class AbstractHeader implements Header {

    protected final short version;

    protected final Schema schema;

    protected final Schema.FieldsIterator fieldsIterator;

    protected final Map<Field, Value> fieldValueBindings = new HashMap<Field, Value>();

    @Override
    public short version() {
        return version;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public int sizeInBytes() {
        int size = 0;
        fieldsIterator.reset();
        while(fieldsIterator.hasNext()) {
            final Field field = fieldsIterator.next();
            size += fieldValueBindings.get(field).sizeInBytes();
        }
        return size;
    }

    public StringBuilder appendTo(final StringBuilder sb) {
        sb.append(this.getClass().getSimpleName())
          .append("{");
        fieldsIterator.reset();
        while(fieldsIterator.hasNext()) {
            sb.append("{");
            final Field field = fieldsIterator.next();
            field.appendTo(sb);
            sb.append("=");
            final Value value = fieldValueBindings.get(field);
            value.appendTo(sb);
            sb.append("}");
        }
        sb.append("}");
        return sb;
    }

    public void reset() {
        fieldsIterator.reset();
        while(fieldsIterator.hasNext()) {
            fieldValueBindings.get(fieldsIterator.next()).reset();
        }
    }

    protected AbstractHeader(final short version,
                             final Schema schema) {
        this.version = version;
        this.schema = schema;
        this.fieldsIterator = schema.getIterator();
        setFieldValueBindings();
    }

    protected abstract void setFieldValueBindings();
}