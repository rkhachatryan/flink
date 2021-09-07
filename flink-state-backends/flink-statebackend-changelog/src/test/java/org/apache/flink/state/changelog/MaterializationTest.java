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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.util.IOUtils;

import org.junit.Test;

import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Test for {@link ChangelogKeyedStateBackend} materialization. */
public class MaterializationTest
        extends /* todo: test with other backends? don't extend ChangelogDelegateHashMapTest? */ ChangelogDelegateHashMapTest {

    @Test
    public void testMaterializedRestore() throws Exception {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

        TypeInformation<TestPojo> pojoType = new GenericTypeInfo<>(TestPojo.class);
        ValueStateDescriptor<TestPojo> kvId = new ValueStateDescriptor<>("id", pojoType);

        ChangelogKeyedStateBackend<Integer> backend =
                (ChangelogKeyedStateBackend<Integer>)
                        createKeyedBackend(IntSerializer.INSTANCE, env);

        MaterializationManager mm =
                new MaterializationManager(
                        Executors.newFixedThreadPool(
                                /* just to demo that no assumption required about threads */
                                10),
                        (msg, err) -> {
                            err.printStackTrace();
                            fail(err.getMessage());
                        },
                        100,
                        0,
                        backend);
        try {
            SequenceNumber materializedTo = backend.getMaterializedTo();
            mm.start();

            ValueState<TestPojo> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            state.update(new TestPojo("u1", 1));

            backend.setCurrentKey(2);
            state.update(new TestPojo("u2", 2));

            materializedTo = materialize(backend, materializedTo);

            backend.setCurrentKey(2);
            state.update(new TestPojo("u2", 22));

            backend.setCurrentKey(3);
            state.update(new TestPojo("u3", 3));

            materialize(backend, materializedTo);

            KeyedStateHandle snapshot =
                    runSnapshot(
                            backend.snapshot(
                                    682375462378L,
                                    2,
                                    createStreamFactory(),
                                    CheckpointOptions.forCheckpointWithDefaultLocation()),
                            sharedStateRegistry);

            IOUtils.closeQuietly(backend);
            backend.dispose();

            // ============================ restore snapshot ===============================

            env.getExecutionConfig().registerKryoType(TestPojo.class);

            backend =
                    (ChangelogKeyedStateBackend<Integer>)
                            restoreKeyedBackend(IntSerializer.INSTANCE, snapshot, env);
            snapshot.discardState();

            state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            assertEquals(state.value(), new TestPojo("u1", 1));

            backend.setCurrentKey(2);
            assertEquals(new TestPojo("u2", 22), state.value());

            backend.setCurrentKey(3);
            assertEquals(state.value(), new TestPojo("u3", 3));
        } finally {
            IOUtils.closeQuietly(mm::close);
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    private SequenceNumber materialize(
            ChangelogKeyedStateBackend<Integer> backend, SequenceNumber materializedTo)
            throws InterruptedException {
        while (materializedTo.compareTo(backend.getMaterializedTo()) >= 0) {
            Thread.sleep(10);
        }
        return backend.getMaterializedTo();
    }
}
