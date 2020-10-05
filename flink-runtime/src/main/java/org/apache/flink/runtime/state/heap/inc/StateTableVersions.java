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

package org.apache.flink.runtime.state.heap.inc;

import org.apache.flink.runtime.state.heap.StateUID;

import java.util.Map;
import java.util.Optional;

class StateTableVersions {
	StateTableVersions(Map<StateUID, StateMapVersions> stateVersions) {
		this.stateVersions = stateVersions;
	}

	private final Map<StateUID, StateMapVersions> stateVersions;

	public Optional<StateMapVersions> getForState(StateUID stateUID) {
		return Optional.ofNullable(stateVersions.get(stateUID));
	}

	@Override
	public String toString() {
		return "stateVersions=" + stateVersions;
	}
}
