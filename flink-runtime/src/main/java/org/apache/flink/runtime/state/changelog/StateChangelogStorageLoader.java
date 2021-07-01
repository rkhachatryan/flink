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

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterators.concat;

/** A thin wrapper around {@link PluginManager} to load {@link StateChangelogStorage}. */
@Internal
public class StateChangelogStorageLoader {

    private static final Logger LOG = LoggerFactory.getLogger(StateChangelogStorageLoader.class);

    /** Object used to protect calls to specific methods. */
    private static final ReentrantLock LOCK = new ReentrantLock(true);

    /**
     * Mapping of state changelog storage identifier to the corresponding storage factories,
     * populated in {@link StateChangelogStorageLoader#initialize(PluginManager)}.
     */
    private static final HashMap<String, StateChangelogStorageFactory>
            STATE_CHANGELOG_STORAGE_FACTORIES = new HashMap<>();

    static {
        // Guarantee to trigger once.
        initialize(null);
    }

    public static void initialize(PluginManager pluginManager) {
        LOCK.lock();
        try {
            STATE_CHANGELOG_STORAGE_FACTORIES.clear();
            Iterator<StateChangelogStorageFactory> iterator =
                    pluginManager == null
                            ? ServiceLoader.load(StateChangelogStorageFactory.class).iterator()
                            : concat(
                                    pluginManager.load(StateChangelogStorageFactory.class),
                                    ServiceLoader.load(StateChangelogStorageFactory.class)
                                            .iterator());
            iterator.forEachRemaining(
                    factory ->
                            STATE_CHANGELOG_STORAGE_FACTORIES.putIfAbsent(
                                    factory.getIdentifier(), factory));
            LOG.info(
                    "StateChangelogStorageLoader initialized with shortcut names {{}}.",
                    String.join(",", STATE_CHANGELOG_STORAGE_FACTORIES.keySet()));
        } finally {
            LOCK.unlock();
        }
    }

    @SuppressWarnings({"rawtypes"})
    public static StateChangelogStorage load(Configuration configuration) {
        final String identifier =
                configuration.getString(CheckpointingOptions.STATE_CHANGE_LOG_STORAGE);
        LOCK.lock();
        try {
            StateChangelogStorageFactory factory =
                    STATE_CHANGELOG_STORAGE_FACTORIES.get(identifier);
            if (factory == null) {
                LOG.warn("Cannot find a factory for changelog storage with name '{}'.", identifier);
                return null;
            } else {
                LOG.info("Creating a changelog storage with name '{}'.", identifier);
                return factory.createStorage(configuration);
            }
        } finally {
            LOCK.unlock();
        }
    }
}
