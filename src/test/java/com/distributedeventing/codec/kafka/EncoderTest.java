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
package com.distributedeventing.codec.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import com.distributedeventing.codec.kafka.messages.requests.ApiVersionsRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class EncoderTest {

    private EmbeddedChannel channel;

    @Before
    public void setUp() {
        byte[] clientIdBytes = "adminclient-1".getBytes(ByteUtils.CHARSET_UTF8);
        channel = new EmbeddedChannel(new Encoder(new CorrelationMap(),
                                                  clientIdBytes));
    }

    @After
    public void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    public void testApiVersionsRequestV3Encoding() {
        // This is only a test of the overall encoding process. Encoding of
        // individual requests has been tested separately.
        ApiVersionsRequest request = ApiVersionsRequest.getInstance(3);
        byte[] softwareName = "apache-kafka-java".getBytes(ByteUtils.CHARSET_UTF8);
        byte[] softwareVersion = "2.5.0".getBytes(ByteUtils.CHARSET_UTF8);
        request.clientSoftwareName(softwareName, 0, softwareName.length);
        request.clientSoftwareVersion(softwareVersion, 0, softwareVersion.length);
        assertTrue(channel.writeOutbound(request));
        ByteBuf written = channel.readOutbound();
        String msgHex = "00 00 00 31 00 12 00 03 00 00 00 00 00 0d 61 64 6d 69 6e 63 6c 69 65 6e 74 2d 31 " +
                        "00 12 61 70 61 63 68 65 2d 6b 61 66 6b 61 2d 6a 61 76 61 06 32 2e 35 2e 30 00";
        ByteBuf expectedBuffer = TestUtils.hexToBinary(msgHex);
        assertTrue(expectedBuffer.equals(written));
        written.release();
    }

}