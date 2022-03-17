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

package org.apache.flink.runtime.jobmaster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class TwoPriQueue extends com.google.common.util.concurrent.ForwardingBlockingQueue<Runnable> {
    private static final Logger LOG = LoggerFactory.getLogger(TwoPriQueue.class);
    private final int threshold;

    private final LinkedBlockingQueue<Runnable> loPriQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Runnable> hiPriQueue = new LinkedBlockingQueue<>();

    public TwoPriQueue(int threshold) {
        this.threshold = threshold;
    }

    @Override
    protected BlockingQueue<Runnable> delegate() {
        return loPriQueue;
    }

    @Override
    public boolean offer(Runnable runnable, long timeout, TimeUnit unit)
            throws InterruptedException {
        return queueToAdd(runnable).offer(runnable, timeout, unit);
    }

    @Override
    public void put(Runnable runnable) throws InterruptedException {
        queueToAdd(runnable).put(runnable);
    }

    @Override
    public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queueToRemove().poll(timeout, unit);
    }

    private LinkedBlockingQueue<Runnable> queueToRemove() {
        return loPriQueue.size() > threshold || hiPriQueue.isEmpty() ? loPriQueue : hiPriQueue;
    }

    private LinkedBlockingQueue<Runnable> queueToAdd(Runnable runnable) {
        if (runnable instanceof LoPriRunnable) {
            return loPriQueue;
        } else {
            if (loPriQueue.size() > threshold) {
                LOG.warn(
                        "Will add a high-priority item while there {} low-priority ones",
                        loPriQueue.size());
            }
            return hiPriQueue;
        }
    }

    @Override
    public Runnable take() throws InterruptedException {
        return queueToRemove().take();
    }

    @Override
    public int drainTo(Collection<? super Runnable> c, int maxElements) {
        return queueToRemove().drainTo(c, maxElements);
    }

    @Override
    public int drainTo(Collection<? super Runnable> c) {
        return queueToRemove().drainTo(c);
    }

    @Override
    public int remainingCapacity() {
        return hiPriQueue.remainingCapacity();
    }

    @Override
    public Runnable peek() {
        return queueToRemove().peek();
    }

    @Override
    public Runnable element() {
        return queueToRemove().element();
    }

    @Override
    public boolean offer(Runnable o) {
        return queueToAdd(o).offer(o);
    }

    @Override
    public Runnable poll() {
        return queueToRemove().poll();
    }

    @Override
    public Runnable remove() {
        return queueToRemove().remove();
    }
}
