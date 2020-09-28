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
package com.distributedeventing.codec.kafka.messages.responses.deletetopics;

import com.distributedeventing.codec.kafka.BrokerError;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.*;

public final class DeletableTopicResult extends CompositeValue {

    /** Name of the topic **/
    public static final Field NAME = new Field("name");

     /**
     * Error code associated with deletion. Will be {@link BrokerError#NONE}
     * if no error occurred
     */
    public static final Field ERROR_CODE = new Field("error_code");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V4 = new Schema(NAME,
                                                      ERROR_CODE,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V4};

    private final CompactString name = new CompactString();

    private final Int16 errorCode = new Int16();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static DeletableTopicResult getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new DeletableTopicResult(SCHEMAS[version]);
    }

    public CompactString name() {
        return name;
    }

    public Int16 errorCode() {
        return errorCode;
    }

    public Array<TaggedBytes>.ElementIterator taggedFields() {
        return taggedFields.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(NAME, name);
        fieldValueBindings.put(ERROR_CODE, errorCode);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private DeletableTopicResult(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class DeletableTopicResultFactory implements ValueFactory<DeletableTopicResult> {

        private final short version;

        public DeletableTopicResultFactory(final short version) {
            this.version = version;
        }

        @Override
        public DeletableTopicResult createInstance() {
            return DeletableTopicResult.getInstance(version);
        }
    }
}