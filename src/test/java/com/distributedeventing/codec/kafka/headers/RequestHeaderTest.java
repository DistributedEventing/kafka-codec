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

import com.distributedeventing.codec.kafka.values.Int16;
import com.distributedeventing.codec.kafka.values.Int32;
import com.distributedeventing.codec.kafka.values.NullableString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.distributedeventing.codec.kafka.ApiKey;
import org.junit.Test;

import java.lang.String;

import static com.distributedeventing.codec.kafka.TestUtils.TAGGED_FIELDS;
import static org.junit.Assert.*;

public class RequestHeaderTest {

    private static final Int16 API_KEY = new Int16(ApiKey.METADATA.key());

    private static final Int16 API_VERSION = new Int16((short) 9);

    private static final Int32 CORRELATION_ID = new Int32(15);

    private static final byte[] CLIENT_ID_BYTES = "kafka-codec".getBytes();

    private static final NullableString CLIENT_ID = new NullableString();

    static {
        CLIENT_ID.value(CLIENT_ID_BYTES);
    }

    @Test
    public void testV0() {
        testVersion((short) 0);
    }

    @Test
    public void testV1() {
        testVersion((short) 1);
    }

    @Test
    public void testV2() {
        testVersion((short) 2);
    }

    private void setV2Params(RequestHeader header) {
        setV1Params(header);
    }

    private void setV1Params(RequestHeader header) {
        setV0Params(header);
        header.clientId(CLIENT_ID_BYTES);
    }

    private void setV0Params(RequestHeader header) {
        header.requestApiKey(API_KEY.value());
        header.requestApiVersion(API_VERSION.value());
        header.correlationId(CORRELATION_ID.value());
    }

    private void testVersion(short version) {
        RequestHeader header = RequestHeader.getInstance(version);
        assertEquals(version, header.version());
        assertTrue(RequestHeader.SCHEMAS[version] == header.schema());
        switch (version) {
            case 0:
                setV0Params(header);
                break;
            case 1:
                setV1Params(header);
                break;
            case 2:
                setV2Params(header);
                break;
            default:
                fail(String.format("Version %d is not supported", version));
                break;
        }
        ByteBuf buffer = Unpooled.buffer();
        header.writeTo(buffer);
        ByteBuf expectedBuffer = getExpectedBuffer(version);
        assertEquals(expectedBuffer.readableBytes(), header.sizeInBytes());
        assertTrue(expectedBuffer.equals(buffer));
    }

    private ByteBuf getExpectedBuffer(int version) {
        ByteBuf buffer = Unpooled.buffer();
        API_KEY.writeTo(buffer);
        API_VERSION.writeTo(buffer);
        CORRELATION_ID.writeTo(buffer);
        if (version == 0) {
            return buffer;
        }
        CLIENT_ID.writeTo(buffer);
        if (version == 1) {
            return buffer;
        }
        TAGGED_FIELDS.writeTo(buffer);
        if (version == 2) {
            return buffer;
        }
        throw new AssertionError(String.format("Version %d is not supported", version));
    }
}