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
package com.distributedeventing.codec.kafka.messages.responses.listoffset;

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.Array;
import com.distributedeventing.codec.kafka.values.CompositeValue;
import com.distributedeventing.codec.kafka.values.String;
import com.distributedeventing.codec.kafka.values.ValueFactory;

public final class ListOffsetTopicResponse extends CompositeValue {

    /** Name of the topic **/
    public static final Field NAME = new Field("name");

    /** Partitions of the topic **/
    public static final Field PARTITIONS = new Field("partitions");

    public static final Schema SCHEMA_V4 = new Schema(NAME,
                                                      PARTITIONS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         null,
                                                         SCHEMA_V4};

    private final String name = new String();

    private final Array<ListOffsetPartitionResponse> partitions;
    {
        if (schema == SCHEMA_V4) {
            partitions = new Array<>(new ListOffsetPartitionResponse.ListOffsetPartitionResponseFactory((short) 4));
        } else {
            partitions = null;
        }
    }

    public static ListOffsetTopicResponse getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new ListOffsetTopicResponse(SCHEMAS[version]);
    }

    public String name() {
        return name;
    }

    public Array<ListOffsetPartitionResponse>.ElementIterator partitions() {
        return partitions.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(NAME, name);
        fieldValueBindings.put(PARTITIONS, partitions);
    }

    private ListOffsetTopicResponse(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class ListOffsetTopicResponseFactory implements ValueFactory<ListOffsetTopicResponse> {

        private final short version;

        public ListOffsetTopicResponseFactory(final short version) {
            this.version = version;
        }

        @Override
        public ListOffsetTopicResponse createInstance() {
            return ListOffsetTopicResponse.getInstance(version);
        }
    }
}