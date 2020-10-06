/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/** Provider-dependent sequence number of a {@link LogRecord} (or multiple log records). */
@Internal
public interface SequenceNumber extends Comparable<SequenceNumber> {

    // todo medium: unit-test
    static boolean contains(SequenceNumber from, SequenceNumber to, SequenceNumber inQuestion) {
        return from.compareTo(inQuestion) <= 0 && inQuestion.compareTo(to) < 0;
    }

    SequenceNumber next();

    /** Generic {@link SequenceNumber}. */
    final class GenericSequenceNumber implements SequenceNumber {
        public final long number;

        GenericSequenceNumber(long number) {
            Preconditions.checkArgument(number >= 0);
            this.number = number;
        }

        @Override
        public int compareTo(SequenceNumber o) {
            Preconditions.checkArgument(o instanceof GenericSequenceNumber);
            return Long.compare(this.number, ((GenericSequenceNumber) o).number);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof GenericSequenceNumber)) {
                return false;
            }
            return number == ((GenericSequenceNumber) o).number;
        }

        @Override
        public int hashCode() {
            return Objects.hash(number);
        }

        @Override
        public SequenceNumber next() {
            return SequenceNumber.of(number + 1); // todo low: handle overflow
        }

        @Override
        public String toString() {
            return Long.toString(number);
        }
    }

    static SequenceNumber of(long number) {
        return new GenericSequenceNumber(number);
    }
}
