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

import com.distributedeventing.codec.kafka.values.TaggedBytes;
import com.distributedeventing.codec.kafka.values.UvilArray;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class TestUtils {

    public static final UvilArray<TaggedBytes> TAGGED_FIELDS = new UvilArray<>(new TaggedBytes.TaggedBytesFactory());

    public static ByteBuf hexToBinary(String hexString) {
        ByteBuf buffer = Unpooled.buffer();
        String[] parts = hexString.split(" ");
        for (String part: parts) {
            if (part.length() != 2) {
                throw new RuntimeException(String.format("Expected %s to be 2 characters", part));
            }
            char ch1 = part.charAt(0);
            char ch2 = part.charAt(1);
            byte result = (byte) (toDecimal(ch1) * 16 + toDecimal(ch2));
            buffer.writeByte(result);
        }
        return buffer;
    }

    public static void printBytes(ByteBuf buffer) {
        byte[] bytes = new byte[buffer.readableBytes()];
        buffer.getBytes(0, bytes);
        printBytes(bytes);
    }

    public static void printBytes(byte[] bytes) {
        for (byte b: bytes) {
            System.out.print(" " + b);
        }
        System.out.println();
    }

    private static int toDecimal(char ch) {
        if (ch >= '0' && ch <= '9') {
            return ch - '0';
        } else if (ch >= 'a' && ch <= 'f') {
            return 10 + ch - 'a';
        } else if (ch >= 'A' && ch <= 'F') {
            return 10 + ch - 'A';
        }
        throw new RuntimeException("Not a valid hexadecimal character: " + ch);
    }
}
