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
package com.distributedeventing.codec.kafka.messages.requests.createtopics;

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.*;

public final class CreateableTopicConfig extends CompositeValue {

    /** Name of the config **/
    public static final Field NAME = new Field("name");

    /** Value of the config **/
    public static final Field VALUE = new Field("value");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V5 = new Schema(NAME,
                                                      VALUE,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V5};

    private final CompactString name = new CompactString();

    private final CompactNullableString value = new CompactNullableString();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static CreateableTopicConfig getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new CreateableTopicConfig(SCHEMAS[version]);
    }

    public void name(final byte[] nameBytes) {
        name.value(nameBytes);
    }

    public void name(final byte[] nameBytes,
                     final int offset,
                     final int length) {
        name.value(nameBytes, offset, length);
    }

    public void value(final byte[] valueBytes) {
        value.value(valueBytes);
    }

    public void value(final byte[] valueBytes,
                      final int offset,
                      final int length) {
        value.value(valueBytes, offset, length);
    }

    public void taggedFields(final TaggedBytes...taggedBytes) {
        taggedFields.value(taggedBytes);
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(NAME, name);
        fieldValueBindings.put(VALUE, value);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private CreateableTopicConfig(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class CreateableTopicConfigFactory implements ValueFactory<CreateableTopicConfig> {

        private final short version;

        public CreateableTopicConfigFactory(final short version) {
            this.version = version;
        }

        @Override
        public CreateableTopicConfig createInstance() {
            return CreateableTopicConfig.getInstance(version);
        }
    }
}