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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.InternalReadOnlyKeyContext;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.state.changelog.StateChangeLoggerImpl.StateChangeOperation.ADD;
import static org.apache.flink.state.changelog.StateChangeLoggerImpl.StateChangeOperation.CLEAR;
import static org.apache.flink.state.changelog.StateChangeLoggerImpl.StateChangeOperation.MERGE_NS;
import static org.apache.flink.state.changelog.StateChangeLoggerImpl.StateChangeOperation.REMOVE_ELEMENT;
import static org.apache.flink.state.changelog.StateChangeLoggerImpl.StateChangeOperation.SET;
import static org.apache.flink.state.changelog.StateChangeLoggerImpl.StateChangeOperation.CHANGE_ELEMENT;

public class StateChangeLoggerImpl<Key, State, Ns> implements StateChangeLogger<State, Ns> {

    private final StateChangelogWriter<?> stateChangelogWriter;
    private final InternalReadOnlyKeyContext<Key> keyContext;
    private final TypeSerializer<Key> keySerializer;
    private final TypeSerializer<Ns> namespaceSerializer;
    private final TypeSerializer<State> valueSerializer;

    public StateChangeLoggerImpl(
            TypeSerializer<Key> keySerializer,
            TypeSerializer<Ns> namespaceSerializer,
            TypeSerializer<State> valueSerializer,
            InternalReadOnlyKeyContext<Key> keyContext,
            StateChangelogWriter<?> stateChangelogWriter) {
        this.stateChangelogWriter = stateChangelogWriter;
        this.keyContext = keyContext;
        this.valueSerializer = valueSerializer;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
    }

    @Override
    public void stateUpdated(State newState, Ns ns) throws IOException {
        if (newState == null) {
            stateCleared(ns);
        } else {
            log(serialize(SET, ns, out -> valueSerializer.serialize(newState, out)));
        }
    }

    @Override
    public void stateAdded(State addedState, Ns ns) throws IOException {
        log(serialize(ADD, ns, out -> valueSerializer.serialize(addedState, out)));
    }

    @Override
    public void stateCleared(Ns ns) {
        try {
            log(serialize(CLEAR, ns, out -> {}));
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public void stateMerged(Ns target, Collection<Ns> sources) throws IOException {
        log(
                serialize(
                        MERGE_NS,
                        target,
                        out -> {
                            out.writeInt(sources.size());
                            for (Ns ns : sources) {
                                namespaceSerializer.serialize(ns, out);
                            }
                        }));
    }

    @Override
    public void stateElementChanged(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, Ns ns)
            throws IOException {
        log(serialize(CHANGE_ELEMENT, ns, dataSerializer));
    }

    @Override
    public void stateElementRemoved(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, Ns ns)
            throws IOException {
        log(serialize(REMOVE_ELEMENT, ns, dataSerializer));
    }

    private void log(byte[] bytes) {
        stateChangelogWriter.append(keyContext.getCurrentKeyGroupIndex(), bytes);
    }

    private byte[] serialize(
            StateChangeOperation op,
            Ns ns,
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataWriter)
            throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(out)) {
            keySerializer.serialize(keyContext.getCurrentKey(), wrapper);
            namespaceSerializer.serialize(ns, wrapper);
            wrapper.writeByte(op.code);
            dataWriter.accept(wrapper);
            return out.toByteArray();
        }
    }

    enum StateChangeOperation {
        /** Scope: key + namespace. */
        CLEAR((byte) 0),
        /** Scope: key + namespace. */
        SET((byte) 1),
        /** Scope: key + namespace. */
        ADD((byte) 2),
        /** Scope: key + namespace, also affecting other (source) namespaces */
        MERGE_NS((byte) 3),
        /** Scope: key + namespace + element (e.g. user map key put or list append). */
        CHANGE_ELEMENT((byte) 4),
        /** Scope: key + namespace + element (e.g. user map remove or iterator remove). */
        REMOVE_ELEMENT((byte) 5);
        private final byte code;

        StateChangeOperation(byte code) {
            this.code = code;
        }
    }
}
