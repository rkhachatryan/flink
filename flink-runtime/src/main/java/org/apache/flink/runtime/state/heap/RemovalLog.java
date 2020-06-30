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

package org.apache.flink.runtime.state.heap;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

interface RemovalLog<K, N> {
	void startNewVersion(int newVersion);

	void added(K k, N n);

	void removed(K k, N n);

	/**
	 * The only @ThreadSafe method.
	 */
	void confirmed(int version);

	void truncate();

	Collection<Map<K, Set<N>>> collect(int upToVersion);

	static <K, N> RemovalLog<K, N> noop() {
		return new RemovalLog<K, N>() {
			@Override
			public void startNewVersion(int newVersion) {
			}

			@Override
			public void added(K k, N n) {
			}

			@Override
			public void removed(K k, N n) {
			}

			@Override
			public void confirmed(int version) {
			}

			@Override
			public void truncate() {
			}

			@Override
			public Collection<Map<K, Set<N>>> collect(int upToVersion) {
				return Collections.emptyList();
			}
		};
	}
}
