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
package com.distributedeventing.codec.kafka.messages.requests.metadata;

import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.values.*;
import com.distributedeventing.codec.kafka.Field;

public final class MetadataRequestTopic extends CompositeValue {

    /** The topic name **/
    public static final Field NAME = new Field("name");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V9 = new Schema(NAME,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V9};

    private final CompactString name = new CompactString();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static MetadataRequestTopic getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new MetadataRequestTopic(SCHEMAS[version]);
    }

    public void name(final byte[] nameBytes) {
        name.value(nameBytes);
    }

    public void name(final byte[] nameBytes,
                     final int offset,
                     final int length) {
        name.value(nameBytes, offset, length);
    }

    public void taggedFields(final TaggedBytes...taggedBytes) {
        taggedFields.value(taggedBytes);
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(NAME, name);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private MetadataRequestTopic(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class MetadataRequestTopicFactory implements ValueFactory<MetadataRequestTopic> {

        private final short version;

        public MetadataRequestTopicFactory(final short version) {
            this.version = version;
        }

        @Override
        public MetadataRequestTopic createInstance() {
            return MetadataRequestTopic.getInstance(version);
        }
    }
}