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

import com.distributedeventing.codec.kafka.exceptions.ParseException;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import io.netty.buffer.ByteBuf;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.Array;
import com.distributedeventing.codec.kafka.values.Int32;
import com.distributedeventing.codec.kafka.values.TaggedBytes;
import com.distributedeventing.codec.kafka.values.UvilArray;

public final class ResponseHeader extends AbstractHeader {

    /**
     * Correlation ID of the response. A response is mapped to a request
     * via their correlation ID.
     */
    public static final Field CORRELATION_ID = new Field("correlation_id");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V0 = new Schema(CORRELATION_ID);

    public static final Schema SCHEMA_V1 = new Schema(CORRELATION_ID,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {SCHEMA_V0,
                                                         SCHEMA_V1};

    private final Int32 correlationId = new Int32();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static ResponseHeader  getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new ResponseHeader(version, SCHEMAS[version]);
    }

    public void readFrom(final ByteBuf buffer) throws ParseException {
        fieldsIterator.reset();
        while (fieldsIterator.hasNext()) {
            fieldValueBindings.get(fieldsIterator.next()).readFrom(buffer);
        }
    }

    public Int32 correlationId() {
        return correlationId;
    }

    public Array<TaggedBytes>.ElementIterator taggedFields() {
        return taggedFields.iterator();
    }

    @Override
    protected void setFieldValueBindings() {
        fieldValueBindings.put(CORRELATION_ID, correlationId);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private ResponseHeader(final short version,
                           final Schema schema) {
        super(version,
              schema);
        setFieldValueBindings();
    }
}