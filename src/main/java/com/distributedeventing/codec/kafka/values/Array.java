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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Array<T extends Value> implements Value {

    protected final List<T> elements = new ArrayList<T>();

    protected final ValueFactory<T> elementFactory;

    public Array(final ValueFactory<T> elementFactory) {
        this.elementFactory = elementFactory;
    }

    @Override
    public void writeTo(final ByteBuf buffer) {
        writeLengthTo(buffer);
        writeDataTo(buffer);
    }

    @Override
    public void readFrom(ByteBuf buffer) throws ParseException {
        final int length = readLengthFrom(buffer);
        validateReadLength(length);
        readDataFrom(buffer, length);
    }

    @Override
    public int sizeInBytes() {
        return sizeOfLenInBytes() + sizeOfInstancesInBytes();
    }

    @Override
    public void reset() {
        elements.clear();
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        sb.append("[");
        if (elements.size() > 0) {
            elements.get(0).appendTo(sb);
        }
        for (int i=1; i<elements.size(); ++i) {
            sb.append(", ");
            elements.get(i).appendTo(sb);
        }
        sb.append("]");
        return sb;
    }

    public ElementIterator iterator() {
        return new ElementIterator();
    }

    public void value(final T...values) {
        elements.clear();
        if (values == null) {
            return;
        }
        for (final T value: values) {
            elements.add(value);
        }
    }

    public void add(final T value) {
        elements.add(value);
    }

    public int length() {
        return elements.size();
    }

    protected void writeLengthTo(final ByteBuf buffer) {
        buffer.writeInt(elements.size());
    }

    protected void writeDataTo(final ByteBuf buffer) {
        for (final T element: elements) {
            element.writeTo(buffer);
        }
    }

    protected int readLengthFrom(final ByteBuf buffer) {
        return buffer.readInt();
    }

    protected void validateReadLength(final int length) throws ParseException {
        if (length < 0) {
            throw new ParseException(java.lang.String.format("Length of %s cannot be -ve; length read = %d",
                                                             this.getClass().getName(),
                                                             length));
        }
    }

    protected void readDataFrom(final ByteBuf buffer,
                                final int length) throws ParseException {
        elements.clear();
        for (int i=0; i<length; ++i) {
            final T element = elementFactory.createInstance();
            element.readFrom(buffer);
            elements.add(element);
        }
    }

    protected int sizeOfLenInBytes() {
        return 4;
    }

    protected int sizeOfInstancesInBytes() {
        int len = 0;
        for(final T element: elements) {
            len += element.sizeInBytes();
        }
        return len;
    }

    public class ElementIterator implements Iterator<T> {

        private int idx = 0;

        @Override
        public boolean hasNext() {
            return idx < elements.size();
        }

        @Override
        public T next() {
            return elements.get(idx++);
        }

        public void reset() {
            idx = 0;
        }
    }
}
