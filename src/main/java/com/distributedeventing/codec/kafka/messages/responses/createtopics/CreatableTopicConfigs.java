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
package com.distributedeventing.codec.kafka.messages.responses.createtopics;

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.*;
import com.distributedeventing.codec.kafka.values.Boolean;

public final class CreatableTopicConfigs extends CompositeValue {

    /** Name of the config **/
    public static final Field NAME = new Field("name");

    /** Value of the config **/
    public static final Field VALUE = new Field("value");

    /** Whether the config is read only **/
    public static final Field READ_ONLY = new Field("read_only");

    public static final Field CONFIG_SOURCE = new Field("config_source");

    public static final Field IS_SENSITIVE = new Field("is_sensitive");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V5 = new Schema(NAME,
                                                      VALUE,
                                                      READ_ONLY,
                                                      CONFIG_SOURCE,
                                                      IS_SENSITIVE,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V5};

    private final CompactString name = new CompactString();

    private final CompactNullableString value = new CompactNullableString();

    private final Boolean readOnly = new Boolean();

    private final Int8 configSource = new Int8();

    private final Boolean isSensitive = new Boolean();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static CreatableTopicConfigs getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new CreatableTopicConfigs(SCHEMAS[version]);
    }

    public CompactString name() {
        return name;
    }

    public CompactNullableString value() {
        return value;
    }

    public Boolean readOnly() {
        return readOnly;
    }

    public Int8 configSource() {
        return configSource;
    }

    public Boolean isSensitive() {
        return isSensitive;
    }

    public Array<TaggedBytes>.ElementIterator taggedFields() {
        return taggedFields.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(NAME, name);
        fieldValueBindings.put(VALUE, value);
        fieldValueBindings.put(READ_ONLY, readOnly);
        fieldValueBindings.put(CONFIG_SOURCE, configSource);
        fieldValueBindings.put(IS_SENSITIVE, isSensitive);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private CreatableTopicConfigs(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class CreatableTopicConfigsFactory implements ValueFactory<CreatableTopicConfigs> {

        private final short version;

        public CreatableTopicConfigsFactory(final short version) {
            this.version = version;
        }

        @Override
        public CreatableTopicConfigs createInstance() {
            return CreatableTopicConfigs.getInstance(version);
        }
    }
}