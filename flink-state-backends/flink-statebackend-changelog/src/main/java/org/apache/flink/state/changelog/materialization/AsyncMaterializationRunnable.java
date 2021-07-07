package org.apache.flink.state.changelog.materialization;

import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.RunnableFuture;

public class AsyncMaterializationRunnable implements Runnable, Closeable {
    public static final Logger LOG = LoggerFactory.getLogger(AsyncMaterializationRunnable.class);

    RunnableFuture<SnapshotResult<KeyedStateHandle>> asyncMaterializationRunnable;
    MaterializedSnapshot materializedSnapshot;

    public AsyncMaterializationRunnable(
            RunnableFuture<SnapshotResult<KeyedStateHandle>> asyncMaterializationRunnable,
            MaterializedSnapshot materializedSnapshot
    ) {
        this.asyncMaterializationRunnable = asyncMaterializationRunnable;
        this.materializedSnapshot = materializedSnapshot;
    }

    @Override
    public void run() {
        try {
            FutureUtils.runIfNotDoneAndGet(asyncMaterializationRunnable);
            materializedSnapshot.setMaterializedSnapshot(asyncMaterializationRunnable.get());
        } catch (Exception e) {
            LOG.info(
                    "{} - asynchronous part of materialization could not be completed.",
                    e);

            // clean-up
        }
    }

    @Override
    public void close() throws IOException {

    }
}
