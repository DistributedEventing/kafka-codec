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

import com.distributedeventing.codec.kafka.ByteUtils;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.exceptions.ParseException;
import com.distributedeventing.codec.kafka.values.*;
import io.netty.buffer.ByteBuf;

public final class Record extends CompositeValue {

    public static final RecordFactory FACTORY = new RecordFactory();

    /** Attributes of the record. This field is not used presently **/
    public static final Field ATTRIBUTES = new Field("attributes");

    /** Timestamp delta of the record with respect to {@link RecordBatch#FIRST_TIMESTAMP} */
    public static final Field TIMESTAMP_DELTA = new Field("timestamp_delta");

    /** Offset delta of the record with respect to {@link RecordBatch#BASE_OFFSET} */
    public static final Field OFFSET_DELTA = new Field("offset_delta");

    /** Key of the record. Clients may choose to not provide this */
    public static final Field KEY = new Field("key");

    /** Value of the record. This is the actual data to be produced */
    public static final Field VALUE = new Field("value");

    /** Record level headers **/
    public static final Field HEADERS = new Field("headers");

    public static final Schema SCHEMA = new Schema(ATTRIBUTES,
                                                   TIMESTAMP_DELTA,
                                                   OFFSET_DELTA,
                                                   KEY,
                                                   VALUE,
                                                   HEADERS);

    private final Int8 attributes = new Int8();

    private final VarLong timestampDelta = new VarLong();

    private final VarInt offsetDelta = new VarInt();

    private final NvlBytes key = new NvlBytes();

    private final NvlBytes value = new NvlBytes();

    private final Header.HeaderFactory headerFactory = Header.FACTORY;

    private final VlArray<Header> headers = new VlArray<>(headerFactory);

    public Record() {
        super(SCHEMA);
        setFieldValueBindings();
        applyDefaults();
    }

    @Override
    public void writeTo(final ByteBuf buffer) {
        int length = 0;
        fieldsIterator.reset();
        while (fieldsIterator.hasNext()) {
            length += fieldValueBindings.get(fieldsIterator.next()).sizeInBytes();
        }
        ByteUtils.writeVarInt(length, buffer);
        super.writeTo(buffer);
    }

    @Override
    public void readFrom(final ByteBuf buffer) throws ParseException {
        ByteUtils.readVarInt(buffer);
        super.readFrom(buffer);
    }

    @Override
    public int sizeInBytes() {
        final int length = super.sizeInBytes();
        return ByteUtils.sizeOfVarInt(length) + length;
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        sb.append("Record{");
        super.appendTo(sb);
        sb.append("}");
        return sb;
    }

    public Int8 attributes() {
        return attributes;
    }

    public void attributes(final byte attrs) {
        attributes.value(attrs);
    }

    public VarLong timestampDelta() {
        return timestampDelta;
    }

    public void timestampDelta(final long delta) {
        timestampDelta.value(delta);
    }

    public VarInt offsetDelta() {
        return offsetDelta;
    }

    public void offsetDelta(final int delta) {
        offsetDelta.value(delta);
    }

    public NvlBytes key() {
        return key;
    }

    public void key(final byte[] keyBytes,
                    final int offset,
                    final int value) {
        key.value(keyBytes, offset, value);
    }

    public NvlBytes value() {
        return value;
    }

    public void value(final byte[] valueBytes) {
        value.value(valueBytes);
    }

    public void value(final byte[] valueBytes,
                      final int offset,
                      final int length) {
        value.value(valueBytes, offset, length);
    }

    public Array<Header>.ElementIterator headers() {
        return headers.iterator();
    }

    public Header createHeader() {
        final Header header = headerFactory.createInstance();
        headers.add(header);
        return header;
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(ATTRIBUTES, attributes);
        fieldValueBindings.put(TIMESTAMP_DELTA, timestampDelta);
        fieldValueBindings.put(OFFSET_DELTA, offsetDelta);
        fieldValueBindings.put(KEY, key);
        fieldValueBindings.put(VALUE, value);
        fieldValueBindings.put(HEADERS, headers);
    }

    private void applyDefaults() {
        key.nullify();
    }

    public static final class RecordFactory implements ValueFactory<Record> {

        @Override
        public Record createInstance() {
            return new Record();
        }

        private RecordFactory() {}
    }
}