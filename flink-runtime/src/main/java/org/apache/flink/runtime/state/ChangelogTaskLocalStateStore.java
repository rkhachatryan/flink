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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.LongPredicate;

/** Changelog's implementation of a {@link TaskLocalStateStore}. */
public class ChangelogTaskLocalStateStore extends TaskLocalStateStoreImpl {

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogTaskLocalStateStore.class);

    private static final String CHANGE_LOG_CHECKPOINT_PREFIX = "changelog_chk_";

    /**
     * The mapper of checkpointId and materializationId. (cp3, materializationId2) means cp3 refer
     * to m1.
     */
    private final Map<Long, Long> mapToMaterializationId;

    /** Last checkpointId, to check whether checkpoint is out of order. */
    private long lastCheckpointId = -1L;

    public ChangelogTaskLocalStateStore(
            @Nonnull JobID jobID,
            @Nonnull AllocationID allocationID,
            @Nonnull JobVertexID jobVertexID,
            @Nonnegative int subtaskIndex,
            @Nonnull LocalRecoveryConfig localRecoveryConfig,
            @Nonnull Executor discardExecutor) {
        super(jobID, allocationID, jobVertexID, subtaskIndex, localRecoveryConfig, discardExecutor);
        this.mapToMaterializationId = new HashMap<>();
    }

    private void updateReference(long checkpointId, TaskStateSnapshot localState) {
        if (localState == null) {
            localState = NULL_DUMMY;
        }
        for (Map.Entry<OperatorID, OperatorSubtaskState> subtaskStateEntry :
                localState.getSubtaskStateMappings()) {
            for (KeyedStateHandle keyedStateHandle :
                    subtaskStateEntry.getValue().getManagedKeyedState()) {
                if (keyedStateHandle instanceof ChangelogStateBackendHandle) {
                    ChangelogStateBackendHandle changelogStateBackendHandle =
                            (ChangelogStateBackendHandle) keyedStateHandle;
                    long materializationID = changelogStateBackendHandle.getMaterializationID();
                    if (mapToMaterializationId.getOrDefault(checkpointId, Long.MAX_VALUE)
                            < materializationID) {
                        LOG.info(
                                "Update checkpoint {}, old materializationID {}, new materializationID {}.",
                                checkpointId,
                                mapToMaterializationId.get(checkpointId),
                                materializationID);
                    }
                    mapToMaterializationId.put(checkpointId, materializationID);
                }
            }
        }
    }

    @Override
    public void storeLocalState(long checkpointId, @Nullable TaskStateSnapshot localState) {
        if (checkpointId < lastCheckpointId) {
            LOG.info(
                    "Current checkpoint {} is out of order, smaller than last CheckpointId {}.",
                    lastCheckpointId,
                    checkpointId);
            return;
        } else {
            lastCheckpointId = checkpointId;
        }
        synchronized (lock) {
            updateReference(checkpointId, localState);
        }
        super.storeLocalState(checkpointId, localState);
    }

    @Override
    protected File getCheckpointDirectory(long checkpointId) {
        final File checkpointDirectory =
                localRecoveryConfig
                        .getLocalStateDirectoryProvider()
                        .orElseThrow(
                                () -> new IllegalStateException("Local recovery must be enabled."))
                        .subtaskBaseDirectory(checkpointId);
        File directoryForChangelog =
                new File(checkpointDirectory, CHANGE_LOG_CHECKPOINT_PREFIX + checkpointId);

        if (!directoryForChangelog.exists() && !directoryForChangelog.mkdirs()) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Could not create the checkpoint directory '%s'",
                            directoryForChangelog));
        }

        return directoryForChangelog;
    }

    private void deleteMaterialization(LongPredicate pruningChecker) {
        final Set<Long> materializationToRemove = new HashSet<>();
        synchronized (lock) {
            Iterator<Entry<Long, Long>> iterator = mapToMaterializationId.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, Long> entry = iterator.next();
                long entryCheckpointId = entry.getKey();
                if (pruningChecker.test(entryCheckpointId)) {
                    materializationToRemove.add(entry.getValue());
                    iterator.remove();
                }
            }

            iterator = mapToMaterializationId.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, Long> entry = iterator.next();
                materializationToRemove.remove(entry.getValue());
            }
        }

        for (Long materializationId : materializationToRemove) {
            File materializedDir =
                    localRecoveryConfig
                            .getLocalStateDirectoryProvider()
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Local recovery must be enabled."))
                            .subtaskSpecificCheckpointDirectory(materializationId);

            if (!materializedDir.exists()) {
                continue;
            }
            try {
                deleteDirectory(materializedDir);
            } catch (IOException ex) {
                LOG.warn(
                        "Exception while deleting local state directory of materialized part {} in subtask ({} - {} - {}).",
                        materializedDir,
                        jobID,
                        jobVertexID,
                        subtaskIndex,
                        ex);
            }
        }
    }

    @Override
    public void confirmCheckpoint(long confirmedCheckpointId) {
        // Scenarios:
        //   c1,m1
        //   confirm c1, do nothing.
        //   c2,m1
        //   confirm c2, delete c1, don't delete m1
        //   c3,m2
        //   confirm c3, delete c2, delete m1
        LOG.debug(
                "Received confirmation for checkpoint {} in subtask ({} - {} - {}). Starting to prune history.",
                confirmedCheckpointId,
                jobID,
                jobVertexID,
                subtaskIndex);
        // delete changelog-chk
        pruneCheckpoints(checkpointId -> checkpointId < confirmedCheckpointId, false);

        deleteMaterialization(checkpointId -> checkpointId < confirmedCheckpointId);
    }

    @Override
    public void abortCheckpoint(long abortedCheckpointId) {
        LOG.debug(
                "Received abort information for checkpoint {} in subtask ({} - {} - {}). Starting to prune history.",
                abortedCheckpointId,
                jobID,
                jobVertexID,
                subtaskIndex);

        pruneCheckpoints(checkpointId -> checkpointId == abortedCheckpointId, false);
        deleteMaterialization(checkpointId -> checkpointId == abortedCheckpointId);
    }

    @Override
    public void pruneMatchingCheckpoints(@Nonnull LongPredicate matcher) {
        pruneCheckpoints(matcher, false);
        deleteMaterialization(matcher);
    }

    @Override
    public CompletableFuture<Void> dispose() {
        deleteMaterialization(id -> true);
        synchronized (lock) {
            mapToMaterializationId.clear();
        }
        return super.dispose();
    }

    @Override
    public String toString() {
        return "ChangelogTaskLocalStateStore{"
                + "jobID="
                + jobID
                + ", jobVertexID="
                + jobVertexID
                + ", allocationID="
                + allocationID.toHexString()
                + ", subtaskIndex="
                + subtaskIndex
                + ", localRecoveryConfig="
                + localRecoveryConfig
                + ", storedCheckpointIDs="
                + storedTaskStateByCheckpointID.keySet()
                + ", mapToMaterializationId="
                + mapToMaterializationId.entrySet()
                + '}';
    }
}
