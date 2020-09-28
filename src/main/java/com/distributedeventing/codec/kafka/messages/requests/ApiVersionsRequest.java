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
package com.distributedeventing.codec.kafka.messages.requests;

import com.distributedeventing.codec.kafka.ApiType;
import com.distributedeventing.codec.kafka.Field;
import com.distributedeventing.codec.kafka.Schema;
import com.distributedeventing.codec.kafka.exceptions.UnsupportedVersionException;
import com.distributedeventing.codec.kafka.headers.RequestHeader;
import com.distributedeventing.codec.kafka.values.TaggedBytes;
import com.distributedeventing.codec.kafka.values.CompactString;
import com.distributedeventing.codec.kafka.values.UvilArray;

/**
 * A request to a Kafka broker to find out the APIs and their versions supported
 * by the broker. This is often the first request to a broker, after which versioned
 * requests are sent to the broker.
 */
public final class ApiVersionsRequest extends RequestMessage {

    /** The name of the software that is connecting to Kafka **/
    public static final Field CLIENT_SOFTWARE_NAME = new Field("client_software_name");

    /** The version of the software that is connecting to Kafka **/
    public static final Field CLIENT_SOFTWARE_VERSION = new Field("client_software_version");

    public static final Field TAGGED_FEILDS = new Field("tagged_fields");

    public static final Schema SCHEMA_V3 = new Schema(CLIENT_SOFTWARE_NAME,
                                                      CLIENT_SOFTWARE_VERSION,
                                                      TAGGED_FEILDS);

    public static final Schema[] SCHEMAS = new Schema[] {null,
                                                         null,
                                                         null,
                                                         SCHEMA_V3};

    private final CompactString clientSoftwareName = new CompactString();

    private final CompactString clientSoftwareVersion = new CompactString();

    private final UvilArray<TaggedBytes> taggedFields = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static ApiVersionsRequest getInstance(final int version) {
        if (version < 0 || version >= SCHEMAS.length || SCHEMAS[version] == null) {
            throw new UnsupportedVersionException(version);
        }
        final short ver = (short) version;
        final ApiType apiType = new ApiType.ApiVersions(ver);
        return new ApiVersionsRequest(createRequestHeader(apiType),
                                      SCHEMAS[ver],
                                      apiType);
    }

    private static RequestHeader createRequestHeader(final ApiType apiType) {
        final short headerVersion = (short) (apiType.version() >= 3? 2:  1);
        final RequestHeader header = RequestHeader.getInstance(headerVersion);
        header.requestApiKey(apiType.key().key());
        header.requestApiVersion(apiType.version());
        return header;
    }

    public void clientSoftwareName(final byte[] nameBytes) {
        clientSoftwareName.value(nameBytes, 0, nameBytes.length);
    }

    public void clientSoftwareName(final byte[] nameBytes,
                                   final int offset,
                                   final int length) {
        clientSoftwareName.value(nameBytes, offset, length);
    }

    public void clientSoftwareVersion(final byte[] versionBytes) {
        clientSoftwareVersion.value(versionBytes, 0, versionBytes.length);
    }

    public void clientSoftwareVersion(final byte[] versionBytes,
                                      final int offset,
                                      final int length) {
        clientSoftwareVersion.value(versionBytes, offset, length);
    }

    public void taggedFields(final TaggedBytes...taggedBytes) {
        taggedFields.value(taggedBytes);
    }

    private void setFieldValueBindings() {
        fieldValueBindings.put(CLIENT_SOFTWARE_NAME, clientSoftwareName);
        fieldValueBindings.put(CLIENT_SOFTWARE_VERSION, clientSoftwareVersion);
        fieldValueBindings.put(TAGGED_FEILDS, taggedFields);
    }

    private ApiVersionsRequest(final RequestHeader header,
                               final Schema schema,
                               final ApiType apiType) {
        super(header,
              schema,
              apiType);
        setFieldValueBindings();
    }
}