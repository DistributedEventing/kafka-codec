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
package com.distributedeventing.codec.kafka.messages.responses.metadata;

import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.values.*;

public final class MetadataResponseBroker extends CompositeValue {

    /** The broker ID **/
    public static final Field NODE_ID = new Field("node_id");

    /** The hostname of the broker **/
    public static final Field HOST = new Field("host");

    /** The port at which the broker is listening to **/
    public static final Field PORT = new Field("port");

    /** The rack assinged to the broker **/
    public static final Field RACK = new Field("rack");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V9 = new Schema(NODE_ID,
                                                      HOST,
                                                      PORT,
                                                      RACK,
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

    private final Int32 nodeId = new Int32();

    private final CompactString host = new CompactString();

    private final Int32 port = new Int32();

    private final CompactNullableString rack = new CompactNullableString();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static MetadataResponseBroker getInstance(final short version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        return new MetadataResponseBroker(SCHEMAS[version]);
    }

    public Int32 nodeId() {
        return nodeId;
    }

    public CompactString host() {
        return host;
    }

    public Int32 port() {
        return port;
    }

    public CompactNullableString rack() {
        return rack;
    }

    public Array<TaggedBytes>.ElementIterator taggedFields() {
        return taggedFields.iterator();
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(NODE_ID, nodeId);
        fieldValueBindings.put(HOST, host);
        fieldValueBindings.put(PORT, port);
        fieldValueBindings.put(RACK, rack);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private MetadataResponseBroker(final Schema schema) {
        super(schema);
        setFieldValueBindings();
    }

    public static final class MetadataResponseBrokerFactory implements ValueFactory<MetadataResponseBroker> {

        private final short version;

        public MetadataResponseBrokerFactory(final short version) {
            this.version = version;
        }

        @Override
        public MetadataResponseBroker createInstance() {
            return MetadataResponseBroker.getInstance(version);
        }
    }
}