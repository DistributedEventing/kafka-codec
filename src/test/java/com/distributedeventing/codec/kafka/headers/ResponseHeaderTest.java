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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.distributedeventing.codec.kafka.values.Int32;
import org.junit.Test;

import static com.distributedeventing.codec.kafka.TestUtils.TAGGED_FIELDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ResponseHeaderTest {

    private static final Int32 CORRELATION_ID = new Int32(15);

    @Test
    public void testV0() {
        test((short) 0);
    }

    @Test
    public void testV1() {
        test((short) 1);
    }

    private void test(short version) {
        ByteBuf buffer = getBufferForVersion(version);
        ResponseHeader header = ResponseHeader.getInstance(version);
        try {
            header.readFrom(buffer);
        } catch (ParseException e) {
            fail(e.getMessage());
        }
        switch (version) {
            case 0:
                assertV0Fields(header);
                break;
            case 1:
                assertV1Fields(header);
                break;
            default:
                fail(String.format("Version %d is not supported", version));
                break;
        }
    }

    private void assertV0Fields(ResponseHeader header) {
        assertEquals(CORRELATION_ID, header.correlationId());
    }

    private void assertV1Fields(ResponseHeader header) {
        assertV0Fields(header);
    }

    private ByteBuf getBufferForVersion(short version) {
        ByteBuf buffer = Unpooled.buffer();
        CORRELATION_ID.writeTo(buffer);
        if (version == 0) {
            return buffer;
        }
        TAGGED_FIELDS.writeTo(buffer);
        if (version == 1) {
            return buffer;
        }
        throw new AssertionError(String.format("Version %d is not supported", version));
    }
}