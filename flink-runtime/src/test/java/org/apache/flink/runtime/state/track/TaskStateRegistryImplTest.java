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

package org.apache.flink.runtime.state.track;

import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.track.TaskStateRegistryImpl.StateObjectIDExtractor;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// todo: add more tests for out-of-order, abortion
/** {@link TaskStateRegistryImpl} test. */
public class TaskStateRegistryImplTest {
    private static final String BACKEND_ID_1 = "b1";
    private static final String BACKEND_ID_2 = "b2";
    private static final long CP_ID_1 = 1L;
    private static final long CP_ID_2 = 2L;
    private static final long CP_ID_3 = 3L;
    private static final StateObject STATE_OBJECT_1 = stateObject("s1");
    private static final StateObject STATE_OBJECT_2 = stateObject("s2");
    private static final StateObject STATE_OBJECT_3 = stateObject("s3");

    @Test
    public void testIgnoreUnknownObject() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_2);
                });
    }

    @Test
    public void testIgnoreUnknownCheckpointOnSubsumption() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_2, false);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_2, false);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_2);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_2);
                });
    }

    @Test
    public void testIgnoreDistributedState() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    registry.addDistributedState(
                            new HashSet<>(asList(STATE_OBJECT_1, STATE_OBJECT_2)));

                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_1);

                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_2, registry);
                    registry.checkpointAborted(BACKEND_ID_1, CP_ID_1);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_2);
                });
    }

    @Test
    public void testDiscardWhenUnregistered() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_1);
                },
                STATE_OBJECT_1);
    }

    @Test
    public void testDiscardWhenUnusedImplicitly() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    // cp1 uses state1
                    // cp2 doesn't; but this is not known at the time when it starts
                    // cp3 starts after cp2 snapshot is reported
                    // so cp3 is guaranteed to not use state1
                    // therefore, state1 can discarded after subsuming cp2

                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_1, false);
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_2, false);
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_1, true);
                    register(STATE_OBJECT_2, registry, BACKEND_ID_1);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_2, CP_ID_2, true);

                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_3, false);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_2, CP_ID_3, true);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1); // not yet
                    assertNotDiscarded(cleaner);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_2); // now
                },
                STATE_OBJECT_1);
    }

    @Test
    public void testDiscardWhenSubsumed() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_1);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                },
                STATE_OBJECT_1);
        runWith(
                (registry, cleaner) -> {
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_1);
                },
                STATE_OBJECT_1);
    }

    @Test
    public void testDiscardWhenLaterCheckpointSubsumed() throws Exception {
        int numCheckpoints = 10;
        StateObject[] checkpointStates = new StateObject[numCheckpoints];
        for (int i = 0; i < numCheckpoints; i++) {
            checkpointStates[i] = stateObject(Integer.toString(i));
        }
        runWith(
                (registry, cleaner) -> {
                    for (int i = 0; i < numCheckpoints; i++) {
                        checkpoint(BACKEND_ID_1, i, checkpointStates[i], registry);
                        registry.unregisterUsage(BACKEND_ID_1, checkpointStates[i]);
                    }
                    registry.checkpointSubsumed(BACKEND_ID_1, numCheckpoints - 1);
                },
                checkpointStates);
    }

    @Test
    public void testDiscardWithMultipleBackends() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1, BACKEND_ID_2);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_1);
                    assertNotDiscarded(cleaner);
                    registry.unregisterUsage(BACKEND_ID_2, STATE_OBJECT_1);
                },
                STATE_OBJECT_1);
    }

    @Test
    public void testDiscardWhenSavepointAborted() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_1, true);
                    registry.checkpointAborted(BACKEND_ID_1, CP_ID_1);
                    assertNotDiscarded(cleaner);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_1);
                },
                STATE_OBJECT_1);
    }

    @Test
    public void testDiscardAll() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    register(STATE_OBJECT_2, registry, BACKEND_ID_1);
                    register(STATE_OBJECT_3, registry, BACKEND_ID_1);
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_1, true);
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_2, false);
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_3, false);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_1, true);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_2, false);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_3, CP_ID_3, false);
                    registry.discardAll();
                },
                STATE_OBJECT_2,
                STATE_OBJECT_3);
    }

    @Test
    public void testNoDiscardIfStillUsed() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                });
    }

    @Test
    public void testNoDiscardIfWasCheckpointed() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_1);
                });
    }

    @Test
    public void testNoDiscardIfCheckpointNotSubsumed() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_2, false);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_2, false);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_1);
                });
    }

    @Test
    public void testNoDiscardIfManagedExternally() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_1, true);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_1, true);
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_2, false);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_2, false);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_1);
                    registry.checkpointSubsumed(BACKEND_ID_1, CP_ID_2);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_1);
                });
    }

    @Test
    public void testNoDiscardOnClose() throws Exception {
        runWith(
                (registry, cleaner) ->
                        registry.registerUsage(
                                singleton(BACKEND_ID_1), singletonList(STATE_OBJECT_1)));
    }

    @Test(expected = IllegalStateException.class)
    public void testRequiresStart() throws Exception {
        runWith(
                (registry, cleaner) ->
                        registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_1, false));
    }

    @Test(expected = NoSuchElementException.class)
    @Ignore // todo
    public void testRequiresRegister() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_1, false);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_1, false);
                });
    }

    @Test(expected = NoSuchElementException.class)
    @Ignore // todo
    public void testRequiresReRegister() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_1);
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_1, false);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_1, false);
                });
    }

    @Test(expected = IllegalStateException.class)
    public void testNoDoubleStart() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_1, false);
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_1, false);
                });
    }

    @Test(expected = IllegalStateException.class)
    public void testNoDoubleSnapshot() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_1, false);
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_1, false);
                });
    }

    @Test
    public void testPendingCheckpointPreventsDiscard() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1, BACKEND_ID_2);
                    registry.checkpointStarted(BACKEND_ID_2, CP_ID_1, false);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_1);
                });
    }

    @Test
    public void testDiscardWhenPendingCheckpointSubsumed() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    register(STATE_OBJECT_1, registry, BACKEND_ID_1);
                    registry.checkpointStarted(BACKEND_ID_2, CP_ID_1, false);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_1);
                    registry.checkpointSubsumed(BACKEND_ID_2, CP_ID_1);
                },
                STATE_OBJECT_1);
    }

    @Test
    public void testOutOfOrder() throws Exception {
        runWith(
                (registry, cleaner) -> {
                    checkpoint(BACKEND_ID_1, CP_ID_1, STATE_OBJECT_1, registry);
                    registry.unregisterUsage(BACKEND_ID_1, STATE_OBJECT_1);
                    registry.checkpointStarted(BACKEND_ID_1, CP_ID_2, false);
                    registry.checkpointPerformed(BACKEND_ID_1, STATE_OBJECT_1, CP_ID_2, false);
                });
    }

    private void runWith(
            BiConsumerWithException<TaskStateRegistryImpl<Object>, TestTaskStateCleaner, Exception>
                    test,
            StateObject... expectDiscarded)
            throws Exception {
        runWith(
                test,
                obj -> {
                    // map should be modifiable
                    Map<Object, StateObject> kvHashMap = new HashMap<>();
                    kvHashMap.put(obj, obj);
                    return kvHashMap;
                },
                expectDiscarded);
    }

    private <T> void runWith(
            BiConsumerWithException<TaskStateRegistryImpl<T>, TestTaskStateCleaner, Exception> test,
            StateObjectIDExtractor<T> idExtractor,
            StateObject... expectDiscarded)
            throws Exception {
        HashSet<StateObject> actual;
        try (TestTaskStateCleaner cleaner = new TestTaskStateCleaner();
                TaskStateRegistryImpl<T> registry =
                        new TaskStateRegistryImpl<>(cleaner, idExtractor)) {
            test.accept(registry, cleaner);
            actual = new HashSet<>(cleaner.getDiscarded());
        }
        // assert after try to check close
        assertEquals("Discarded state differs", new HashSet<>(asList(expectDiscarded)), actual);
    }

    private static StateObject stateObject(final String name) {
        return new StateObject() {
            @Override
            public String toString() {
                return name;
            }

            @Override
            public void discardState() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getStateSize() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static class TestTaskStateCleaner implements TaskStateCleaner {
        private final List<StateObject> discarded = new ArrayList<>();

        @Override
        public void discardAsync(StateObject state) {
            discarded.add(state);
        }

        public List<StateObject> getDiscarded() {
            return unmodifiableList(discarded);
        }

        @Override
        public void close() {}
    }

    private void checkpoint(
            String backendId,
            long checkpointId,
            StateObject state,
            TaskStateRegistryImpl<Object> registry) {
        register(state, registry, backendId);
        registry.checkpointStarted(backendId, checkpointId, false);
        registry.checkpointPerformed(backendId, state, checkpointId, false);
    }

    private void register(
            StateObject state, TaskStateRegistryImpl<Object> registry, String... backendIds) {
        registry.registerUsage(new HashSet<>(asList(backendIds)), singletonList(state));
    }

    private void assertNotDiscarded(TestTaskStateCleaner cleaner) {
        assertTrue(cleaner.getDiscarded().isEmpty());
    }
}
