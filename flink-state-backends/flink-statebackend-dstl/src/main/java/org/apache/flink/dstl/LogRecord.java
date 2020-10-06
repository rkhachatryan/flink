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

package org.apache.flink.dstl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Objects;

/**
 * Representation of a state change or an output element.
 * Interpretation depends on the state backend.
 */
@Internal
public interface LogRecord {

	int keyGroup();

	long timestamp();

	byte[] key();

	byte[] value();

	/**
	 * Generic {@link LogRecord}.
	 */
	class GenericLogRecord implements LogRecord {

		private final int keyGroup;
		private final long timestamp;
		private final byte[] key;
		private final byte[] value;

		GenericLogRecord(int keyGroup, long timestamp, byte[] key, byte[] value) {
			Preconditions.checkArgument(keyGroup >= 0);
			Preconditions.checkArgument(timestamp >= 0);
			this.keyGroup = keyGroup;
			this.timestamp = timestamp;
			this.key = Preconditions.checkNotNull(key);
			this.value = Preconditions.checkNotNull(value);
		}

		@Override
		public int keyGroup() {
			return keyGroup;
		}

		@Override
		public long timestamp() {
			return timestamp;
		}

		@Override
		public byte[] key() {
			return key;
		}

		@Override
		public byte[] value() {
			return value;
		}

		@Override
		public String toString() {
			return String.format("keyGroup=%d, valueLen=%s", keyGroup, value.length);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof GenericLogRecord)) {
				return false;
			}
			final GenericLogRecord that = (GenericLogRecord) o;
			return keyGroup == that.keyGroup &&
// todo low: compare timestamp and key?
//				timestamp == that.timestamp &&
//				Arrays.equals(key, that.key) &&
				Arrays.equals(value, that.value);
		}

		@Override
		public int hashCode() {
// todo low: compare timestamp and key?
//			int result = Objects.hash(keyGroup, timestamp);
//			result = 31 * result + Arrays.hashCode(key);
			int result = Objects.hash(keyGroup);
			result = 31 * result + Arrays.hashCode(value);
			return result;
		}
	}

	static LogRecord of(int keyGroup, long timestamp, byte[] key, byte[] value) {
		return new GenericLogRecord(keyGroup, timestamp, key, value);
	}

	static LogRecord of(int keyGroup, byte[] bytes) {
		return of(keyGroup, 0L, new byte[0], bytes);
	}
}
