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
package com.distributedeventing.codec.kafka.values.records;

import com.distributedeventing.codec.kafka.exceptions.ParseException;
import io.netty.buffer.ByteBuf;
import com.distributedeventing.codec.kafka.values.Value;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class RecordBatchArray implements Value {

    private final List<RecordBatch> elements = new ArrayList<RecordBatch>();

    private final RecordBatch.RecordBatchFactory factory = RecordBatch.FACTORY;

    @Override
    public void writeTo(final ByteBuf buffer) {
        final int lengthOffset = buffer.writerIndex();
        buffer.writerIndex(lengthOffset + 4);
        final int dataOffset = buffer.writerIndex();
        for (final RecordBatch element: elements) {
            element.writeTo(buffer);
        }
        final int written = buffer.writerIndex() - dataOffset;
        buffer.setInt(lengthOffset, written);
    }

    @Override
    public void readFrom(final ByteBuf buffer) throws ParseException {
        int length = buffer.readInt();
        validateReadLength(length);
        elements.clear();
        while (length > 0) {
            final RecordBatch element = factory.createInstance();
            elements.add(element);
            final int preIdx = buffer.readerIndex();
            element.readFrom(buffer);
            final int postIdx = buffer.readerIndex();
            length -= (postIdx - preIdx);
        }
    }

    @Override
    public int sizeInBytes() {
        int size = 4;
        for (final RecordBatch element: elements) {
            size += element.sizeInBytes();
        }
        return size;
    }

    @Override
    public void reset() {
        elements.clear();
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        sb.append("RecordBatchArray{");
        for (final RecordBatch element: elements) {
            element.appendTo(sb);
        }
        sb.append("}");
        return sb;
    }

    public ElementIterator iterator() {
        return new ElementIterator();
    }

    public RecordBatch createRecordBatch() {
        final RecordBatch recordBatch = factory.createInstance();
        elements.add(recordBatch);
        return recordBatch;
    }

    private void validateReadLength(final int length) throws ParseException {
        if (length < 0) {
            throw new ParseException(java.lang.String.format("Length of %s cannot be -ve; length read = %d",
                                                             this.getClass().getName(),
                                                             length));
        }
    }

    public class ElementIterator implements Iterator<RecordBatch> {

        private int idx = 0;

        @Override
        public boolean hasNext() {
            return idx < elements.size();
        }

        @Override
        public RecordBatch next() {
            return elements.get(idx++);
        }

        public void reset() {
            idx = 0;
        }
    }
}