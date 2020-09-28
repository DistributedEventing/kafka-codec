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

public class Scaler {

    public static final Scaler DEFAULT_SCALER = new Scaler(2, 1024, 256);

    private final int multiplier;

    private final int exponentialCapacity;

    private final int addendum;

    public Scaler(final int multiplier,
                  final int exponentialCapacity,
                  final int addendum) {
        this.multiplier = multiplier;
        this.exponentialCapacity = exponentialCapacity;
        this.addendum = addendum;
    }

    public int scale(final int value) {
        if (value ==0 ) {
            throw new RuntimeException("Cannot scale 0");
        }
        long result;
        if (value >= exponentialCapacity) {
            result = scaleByAddition(value);
        } else {
            result = scaleByMultiplication(value);
        }
        if (result > Integer.MAX_VALUE) {
            // will most likely exhaust memory before this happens
            throw new OverflowException(String.format("Result of scaling exceeds maximum integer; value=%d, " +
                                                      "multiplier=%d, exponentialCapacity=%d, addendum=%d",
                                                      value, multiplier, exponentialCapacity, addendum));
        }
        return (int) result;
    }

    private long scaleByMultiplication(final int value) {
        long result = value * multiplier;
        if (result <= exponentialCapacity) {
            return result;
        }
        return scaleByAddition(value);
    }

    private long scaleByAddition(final int value) {
        return value + addendum;
    }

    public static final class OverflowException extends RuntimeException {

        public OverflowException() {
            super ();
        }

        public OverflowException(final String message) {
            super (message);
        }

        public OverflowException(final Throwable cause) {
            super (cause);
        }

        public OverflowException(final String message,
                                 final Throwable cause) {
            super (message, cause);
        }
    }
}