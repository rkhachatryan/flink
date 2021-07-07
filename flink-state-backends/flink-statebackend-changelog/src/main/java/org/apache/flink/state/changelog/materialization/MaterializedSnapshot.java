package org.apache.flink.state.changelog.materialization;

import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;

import javax.annotation.concurrent.GuardedBy;

public class MaterializedSnapshot {
    @GuardedBy("materializedSnapshot")
    private SnapshotResult<KeyedStateHandle> materializedSnapshot = SnapshotResult.empty();

    public synchronized void setMaterializedSnapshot(SnapshotResult<KeyedStateHandle> materializedSnapshot) {
        this.materializedSnapshot = materializedSnapshot;
    }

    public synchronized SnapshotResult<KeyedStateHandle> getMaterializedSnapshot() {
        return materializedSnapshot;
    }

    public synchronized boolean isEmpty() {
        return materializedSnapshot.equals(SnapshotResult.empty());
    }
}
