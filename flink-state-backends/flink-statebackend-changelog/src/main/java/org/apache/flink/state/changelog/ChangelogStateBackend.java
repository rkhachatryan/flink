/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.inmemory.InMemoryStateChangelogStorage;
import org.apache.flink.runtime.state.delegate.DelegatingStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.changelog.restore.ChangelogBackendRestoreOperation;
import org.apache.flink.state.changelog.restore.ChangelogBackendRestoreOperation.BaseBackendBuilder;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * This state backend holds the working state in the underlying delegatedStateBackend, and forwards
 * state changes to State Changelog.
 */
public class ChangelogStateBackend implements DelegatingStateBackend, ConfigurableStateBackend {

    private static final long serialVersionUID = 1000L;

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogStateBackend.class);

    private final StateBackend delegatedStateBackend;
    private final StateChangelogStorage<?> stateChangelogWriterFactory;

    public ChangelogStateBackend(
            StateBackend stateBackend, StateChangelogStorage<?> stateChangelogWriterFactory) {
        this.delegatedStateBackend = Preconditions.checkNotNull(stateBackend);
        this.stateChangelogWriterFactory = stateChangelogWriterFactory;

        Preconditions.checkArgument(
                !(stateBackend instanceof DelegatingStateBackend),
                "Recursive Delegation is not supported.");

        LOG.info(
                "ChangelogStateBackend is used, delegating {}.",
                delegatedStateBackend.getClass().getSimpleName());
    }

    @Override
    public <K> ChangelogKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {
        return restore(
                env,
                operatorIdentifier,
                keyGroupRange,
                ttlTimeProvider,
                stateHandles,
                baseHandles ->
                        (AbstractKeyedStateBackend<K>)
                                delegatedStateBackend.createKeyedStateBackend(
                                        env,
                                        jobID,
                                        operatorIdentifier,
                                        keySerializer,
                                        numberOfKeyGroups,
                                        keyGroupRange,
                                        kvStateRegistry,
                                        ttlTimeProvider,
                                        metricGroup,
                                        baseHandles,
                                        cancelStreamRegistry));
    }

    @Override
    public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry,
            double managedMemoryFraction)
            throws Exception {
        return restore(
                env,
                operatorIdentifier,
                keyGroupRange,
                ttlTimeProvider,
                stateHandles,
                baseHandles ->
                        (AbstractKeyedStateBackend<K>)
                                delegatedStateBackend.createKeyedStateBackend(
                                        env,
                                        jobID,
                                        operatorIdentifier,
                                        keySerializer,
                                        numberOfKeyGroups,
                                        keyGroupRange,
                                        kvStateRegistry,
                                        ttlTimeProvider,
                                        metricGroup,
                                        baseHandles,
                                        cancelStreamRegistry,
                                        managedMemoryFraction));
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier,
            @Nonnull Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {
        return delegatedStateBackend.createOperatorStateBackend(
                env, operatorIdentifier, stateHandles, cancelStreamRegistry);
    }

    @Override
    public boolean useManagedMemory() {
        return delegatedStateBackend.useManagedMemory();
    }

    @Override
    public StateBackend getDelegatedStateBackend() {
        return delegatedStateBackend;
    }

    @Override
    public StateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {

        if (delegatedStateBackend instanceof ConfigurableStateBackend) {
            return new ChangelogStateBackend(
                    ((ConfigurableStateBackend) delegatedStateBackend)
                            .configure(config, classLoader),
                    this.stateChangelogWriterFactory);
        }

        return this;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <K> ChangelogKeyedStateBackend<K> restore(
            Environment env,
            String operatorIdentifier,
            KeyGroupRange keyGroupRange,
            TtlTimeProvider ttlTimeProvider,
            Collection<KeyedStateHandle> stateHandles,
            BaseBackendBuilder<K> baseBackendBuilder)
            throws Exception {
        // todo: FLINK-21804 get from Environment.getTaskStateManager
        InMemoryStateChangelogStorage changelogStorage = new InMemoryStateChangelogStorage();
        return ChangelogBackendRestoreOperation.restore(
                changelogStorage.createReader(),
                env.getUserCodeClassLoader().asClassLoader(),
                castHandles(stateHandles),
                baseBackendBuilder,
                (baseBackend, baseState) ->
                        new ChangelogKeyedStateBackend(
                                baseBackend,
                                env.getExecutionConfig(),
                                ttlTimeProvider,
                                changelogStorage.createWriter(operatorIdentifier, keyGroupRange),
                                baseState));
    }

    private Collection<ChangelogStateBackendHandle> castHandles(
            Collection<KeyedStateHandle> stateHandles) {
        return stateHandles.stream()
                .map(keyedStateHandle -> (ChangelogStateBackendHandle) keyedStateHandle)
                .collect(Collectors.toList());
    }
}
