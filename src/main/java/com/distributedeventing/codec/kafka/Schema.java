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

import java.util.Iterator;

public final class Schema {

    private final Field[] fields;

    public Schema(final Field...fields) {
        this.fields = fields;
    }

    public FieldsIterator getIterator() {
        return new FieldsIterator();
    }

    public class FieldsIterator implements Iterator<Field> {

        private int idx = 0;

        @Override
        public boolean hasNext() {
            return idx < fields.length;
        }

        @Override
        public Field next() {
            return fields[idx++];
        }

        public void reset() {
            idx = 0;
        }
    }
}
