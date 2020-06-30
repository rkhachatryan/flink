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

import org.apache.flink.runtime.state.heap.inc.StateDiff.ReplacingDiff;

// todo: handle null state
// todo: prevent journaling journals
// todo: enforce lifecycle: single call to get diff
interface StateJournalFactory<S, D extends StateDiff<S>, J extends StateJournal<S, D>> {

	J createJournal(S state, boolean wasCleared, boolean isStateExposed);

	static <S> StateJournalFactory<S, ReplacingDiff<S>, StateJournal<S, ReplacingDiff<S>>> replacing() {
		return (state, wasCleared, isStateExposed) -> new StateJournal<S, ReplacingDiff<S>>() {
			@Override
			public S getJournaledState() {
				return state;
			}

			@Override
			public ReplacingDiff<S> getDiff() {
				return new ReplacingDiff<>(state);
			}

			@Override
			public boolean isStateExposed() {
				return isStateExposed;
			}

			@Override
			public boolean wasCleared() {
				return true;
			}

			@Override
			public String toString() {
				return "replacing journal, state=" + state;
			}
		};
	}
}
