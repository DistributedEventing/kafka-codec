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

import com.distributedeventing.codec.kafka.ByteUtils;
import com.distributedeventing.codec.kafka.TestUtils;
import com.distributedeventing.codec.kafka.values.CompactNullableString;
import com.distributedeventing.codec.kafka.values.CompactString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.distributedeventing.codec.kafka.messages.requests.RequestTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CreateableTopicConfigTest {

    @Test
    public void testV5_1() {
        ByteBuf buffer = Unpooled.buffer();
        ConfigV5_1.CONFIG.writeTo(buffer);
        ByteBuf expectedBuffer = ConfigV5_1.getExpectedBuffer();
        Assert.assertEquals(expectedBuffer.readableBytes(), ConfigV5_1.CONFIG.sizeInBytes());
        assertTrue(expectedBuffer.equals(buffer));
    }

    @Test
    public void testV5_2() {
        ByteBuf buffer = Unpooled.buffer();
        ConfigV5_2.CONFIG.writeTo(buffer);
        ByteBuf expectedBuffer = ConfigV5_2.getExpectedBuffer();
        Assert.assertEquals(expectedBuffer.readableBytes(), ConfigV5_2.CONFIG.sizeInBytes());
        assertTrue(expectedBuffer.equals(buffer));
    }

    public static final class ConfigV5_1 {

        public static final byte[] NAME_BYTES = "prop1".getBytes(ByteUtils.CHARSET_UTF8);

        public static final CompactString NAME = new CompactString();
        static {
            NAME.value(NAME_BYTES);
        }

        public static final byte[] VALUE_BYTES = "value1".getBytes(ByteUtils.CHARSET_UTF8);

        public static final CompactNullableString VALUE = new CompactNullableString();
        static {
            VALUE.value(VALUE_BYTES);
        }

        public static final CreateableTopicConfig CONFIG = CreateableTopicConfig.getInstance((short) 5);
        static {
            CONFIG.name(NAME_BYTES);
            CONFIG.value(VALUE_BYTES);
        }

        public static ByteBuf getExpectedBuffer() {
            return RequestTestUtils.getExpectedBuffer(NAME,
                                                      VALUE,
                                                      TestUtils.TAGGED_FIELDS);
        }
    }

    public static final class ConfigV5_2 {

        public static final byte[] NAME_BYTES = "prop2".getBytes(ByteUtils.CHARSET_UTF8);

        public static final CompactString NAME = new CompactString();
        static {
            NAME.value(NAME_BYTES);
        }

        public static final CompactNullableString VALUE = new CompactNullableString();
        static {
            VALUE.nullify();
        }

        public static final CreateableTopicConfig CONFIG = CreateableTopicConfig.getInstance((short) 5);
        static {
            CONFIG.name(NAME_BYTES, 0, NAME_BYTES.length);
            CONFIG.value(null, 0, 0);
        }

        public static ByteBuf getExpectedBuffer() {
            return RequestTestUtils.getExpectedBuffer(NAME,
                                                      VALUE,
                                                      TestUtils.TAGGED_FIELDS);
        }
    }
}