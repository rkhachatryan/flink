/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Random;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Test for {@link TaskStateSnapshotTest}.
 */
public class TaskStateSnapshotTest extends TestLogger {

	@Test
	public void hasNoState() {
		Assert.assertFalse(TaskStateSnapshot.EMPTY.hasState());
		OperatorSubtaskState emptyOperatorSubtaskState = new OperatorSubtaskState();
		Assert.assertFalse(emptyOperatorSubtaskState.hasState());
		Assert.assertFalse(new TaskStateSnapshot(new OperatorID(), emptyOperatorSubtaskState).hasState());
	}

	@Test
	public void hasState() {
		Random random = new Random(0x42);
		OperatorStateHandle stateHandle = StateHandleDummyUtil.createNewOperatorStateHandle(2, random);
		OperatorSubtaskState nonEmptyOperatorSubtaskState = new OperatorSubtaskState(
			stateHandle,
			null,
			null,
			null
		);

		Assert.assertTrue(nonEmptyOperatorSubtaskState.hasState());
		Assert.assertTrue(new TaskStateSnapshot(new OperatorID(), nonEmptyOperatorSubtaskState).hasState());
	}

	@Test
	public void discardState() throws Exception {
		OperatorID operatorID_1 = new OperatorID();
		OperatorID operatorID_2 = new OperatorID();

		OperatorSubtaskState operatorSubtaskState_1 = mock(OperatorSubtaskState.class);
		OperatorSubtaskState operatorSubtaskState_2 = mock(OperatorSubtaskState.class);

		snapshot(operatorSubtaskState_1, operatorSubtaskState_2).discardState();

		verify(operatorSubtaskState_1).discardState();
		verify(operatorSubtaskState_2).discardState();
	}

	@Test
	public void getStateSize() {
		Random random = new Random(0x42);
		Assert.assertEquals(0, TaskStateSnapshot.EMPTY.getStateSize());

		OperatorSubtaskState emptyOperatorSubtaskState = new OperatorSubtaskState();
		Assert.assertFalse(emptyOperatorSubtaskState.hasState());
		Assert.assertEquals(0, new TaskStateSnapshot(new OperatorID(), emptyOperatorSubtaskState).getStateSize());

		OperatorStateHandle stateHandle_1 = StateHandleDummyUtil.createNewOperatorStateHandle(2, random);
		OperatorSubtaskState nonEmptyOperatorSubtaskState_1 = new OperatorSubtaskState(
			stateHandle_1,
			null,
			null,
			null
		);

		OperatorStateHandle stateHandle_2 = StateHandleDummyUtil.createNewOperatorStateHandle(2, random);
		OperatorSubtaskState nonEmptyOperatorSubtaskState_2 = new OperatorSubtaskState(
			null,
			stateHandle_2,
			null,
			null
		);

		TaskStateSnapshot taskStateSnapshot = snapshot(nonEmptyOperatorSubtaskState_1, nonEmptyOperatorSubtaskState_2);

		long totalSize = stateHandle_1.getStateSize() + stateHandle_2.getStateSize();
		Assert.assertEquals(totalSize, taskStateSnapshot.getStateSize());
	}

	private TaskStateSnapshot snapshot(OperatorSubtaskState... states) {
		HashMap<OperatorID, OperatorSubtaskState> map = new HashMap<>();
		for (OperatorSubtaskState state: states) {
			map.put(new OperatorID(), state);
		}
		return new TaskStateSnapshot(map);
	}
}
