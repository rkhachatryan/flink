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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;

final class CheckpointingOperation {

	static void execute(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics,
			CheckpointStreamFactory storageLocation,
			StreamTask<?, ?> task) throws Exception {

		Preconditions.checkNotNull(task);
		Preconditions.checkNotNull(checkpointMetaData);
		Preconditions.checkNotNull(checkpointOptions);
		Preconditions.checkNotNull(checkpointMetrics);
		Preconditions.checkNotNull(storageLocation);

		long startSyncPartNano = System.nanoTime();

		HashMap<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress = new HashMap<>(task.operatorChain.getNumberOfOperators());
		try {
			for (StreamOperatorWrapper<?, ?> operatorWrapper : task.operatorChain.getAllOperators(true)) {
				StreamOperator<?> op = operatorWrapper.getStreamOperator();
				OperatorSnapshotFutures snapshotInProgress = op.snapshotState(
					checkpointMetaData.getCheckpointId(),
					checkpointMetaData.getTimestamp(),
					checkpointOptions,
					storageLocation);
				operatorSnapshotsInProgress.put(op.getOperatorID(), snapshotInProgress);
			}

			if (StreamTask.LOG.isDebugEnabled()) {
				StreamTask.LOG.debug("Finished synchronous checkpoints for checkpoint {} on task {}",
					checkpointMetaData.getCheckpointId(), task.getName());
			}

			long startAsyncPartNano = System.nanoTime();

			checkpointMetrics.setSyncDurationMillis((startAsyncPartNano - startSyncPartNano) / 1_000_000);

			// we are transferring ownership over snapshotInProgressList for cleanup to the thread, active on submit
			AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(
				task,
				operatorSnapshotsInProgress,
				checkpointMetaData,
				checkpointMetrics,
				startAsyncPartNano);

			task.getCancelables().registerCloseable(asyncCheckpointRunnable);
			task.getAsyncOperationsThreadPool().execute(asyncCheckpointRunnable);

			if (StreamTask.LOG.isDebugEnabled()) {
				StreamTask.LOG.debug("{} - finished synchronous part of checkpoint {}. " +
						"Alignment duration: {} ms, snapshot duration {} ms",
					task.getName(), checkpointMetaData.getCheckpointId(),
					checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
					checkpointMetrics.getSyncDurationMillis());
			}
		} catch (Exception ex) {
			// Cleanup to release resources
			for (OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
				if (null != operatorSnapshotResult) {
					try {
						operatorSnapshotResult.cancel();
					} catch (Exception e) {
						StreamTask.LOG.warn("Could not properly cancel an operator snapshot result.", e);
					}
				}
			}

			if (StreamTask.LOG.isDebugEnabled()) {
				StreamTask.LOG.debug("{} - did NOT finish synchronous part of checkpoint {}. " +
						"Alignment duration: {} ms, snapshot duration {} ms",
					task.getName(), checkpointMetaData.getCheckpointId(),
					checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
					checkpointMetrics.getSyncDurationMillis());
			}

			if (checkpointOptions.getCheckpointType().isSynchronous()) {
				// in the case of a synchronous checkpoint, we always rethrow the exception,
				// so that the task fails.
				// this is because the intention is always to stop the job after this checkpointing
				// operation, and without the failure, the task would go back to normal execution.
				throw ex;
			} else {
				task.getEnvironment().declineCheckpoint(checkpointMetaData.getCheckpointId(), ex);
			}
		}
	}

}
