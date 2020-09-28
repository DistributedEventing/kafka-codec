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
package com.distributedeventing.codec.kafka.messages;

import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.headers.Header;
import com.distributedeventing.codec.kafka.values.Value;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractMessage implements Message {

    protected final Header header;

    protected final Schema schema;

    protected final Schema.FieldsIterator fieldsIterator;

    protected final Map<Field, Value> fieldValueBindings = new HashMap<Field, Value>();

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public int sizeInBytes() {
        int size = header.sizeInBytes();
        fieldsIterator.reset();
        while (fieldsIterator.hasNext()) {
            size += fieldValueBindings.get(fieldsIterator.next()).sizeInBytes();
        }
        return size;
    }

    @Override
    public void reset() {
        header.reset();
        fieldsIterator.reset();
        while (fieldsIterator.hasNext()) {
            fieldValueBindings.get(fieldsIterator.next()).reset();
        }
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        sb.append(this.getClass().getSimpleName())
          .append("{");
        fieldsIterator.reset();
        while(fieldsIterator.hasNext()) {
            sb.append("{");
            final Field field = fieldsIterator.next();
            field.appendTo(sb);
            sb.append("=");
            fieldValueBindings.get(field).appendTo(sb);
            sb.append("}");
        }
        sb.append("}");
        return sb;
    }

    protected AbstractMessage(final Header header,
                              final Schema schema) {
        this.header = header;
        this.schema = schema;
        this.fieldsIterator = schema.getIterator();
    }
}