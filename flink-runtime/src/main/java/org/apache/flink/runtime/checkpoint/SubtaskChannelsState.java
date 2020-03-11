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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.state.ChannelStateHandle;
import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Container for {@link org.apache.flink.runtime.state.ChannelStateHandle channel state}.
 */
public class SubtaskChannelsState implements CompositeStateHandle {
	private static final long serialVersionUID = 1L;

	public static final SubtaskChannelsState EMPTY = new SubtaskChannelsState(Collections.emptyList());

	private final Collection<ChannelStateHandle> stateHandles;

	public SubtaskChannelsState(Collection<ChannelStateHandle> stateHandles) {
		this.stateHandles = Collections.unmodifiableList(new ArrayList<>(stateHandles));
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {
		for (ChannelStateHandle handle : stateHandles) {
			handle.registerSharedStates(stateRegistry);
		}
	}

	@Override
	public void discardState() throws Exception {
		for (ChannelStateHandle handle : stateHandles) {
			handle.discardState();
		}
	}

	@Override
	public long getStateSize() {
		int size = 0;
		for (ChannelStateHandle handle : stateHandles) {
			size += handle.getStateSize();
		}
		return size;
	}

	public boolean hasState() {
		return getStateSize() > 0;
	}

	public Collection<ChannelStateHandle> getStateHandles() {
		return stateHandles;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SubtaskChannelsState that = (SubtaskChannelsState) o;
		return stateHandles.equals(that.stateHandles);
	}

	@Override
	public int hashCode() {
		return Objects.hash(stateHandles);
	}

	@Override
	public String toString() {
		return "SubtaskChannelsState{" + "stateHandles=" + stateHandles + '}';
	}
}
