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

import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.values.CompositeValue;
import com.distributedeventing.codec.kafka.values.NvlBytes;
import com.distributedeventing.codec.kafka.values.ValueFactory;
import com.distributedeventing.codec.kafka.values.VlBytes;

public final class Header extends CompositeValue {

    public static final HeaderFactory FACTORY = new HeaderFactory();

    public static final Field KEY = new Field("key");

    public static final Field VALUE = new Field("value");

    public static final Schema SCHEMA = new Schema(KEY, VALUE);

    private final VlBytes key = new VlBytes();

    private final NvlBytes value = new NvlBytes();

    public static Header getInstance() {
        return new Header(SCHEMA);
    }

    public VlBytes key() {
        return key;
    }

    public void key(final byte[] keyBytes,
                    final int offset,
                    final int length) {
        key.value(keyBytes, offset, length);
    }

    public NvlBytes value() {
        return value;
    }

    public void value(final byte[] valueBytes,
                      final int offset,
                      final int length) {
        value.value(valueBytes, offset, length);
    }

    @Override
    public StringBuilder appendTo(final StringBuilder sb) {
        sb.append("Header{");
        super.appendTo(sb);
        sb.append("}");
        return sb;
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(KEY, key);
        fieldValueBindings.put(VALUE, value);
    }

    private Header(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class HeaderFactory implements ValueFactory<Header> {

        @Override
        public Header createInstance() {
            return Header.getInstance();
        }

        private HeaderFactory() {}
    }
}