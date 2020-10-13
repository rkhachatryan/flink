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

package org.apache.flink.dstl.fs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.dstl.LogId.UuidLogId;
import org.apache.flink.dstl.LogPointer;
import org.apache.flink.dstl.SequenceNumber;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE;

class FsLogPersister {
	private static final Logger LOG = LoggerFactory.getLogger(FsLogPersister.class);
	private static final ThreadFactory THREAD_FACTORY = new ThreadFactory() {
		private final AtomicInteger threadCounter = new AtomicInteger();

		@Override
		public Thread newThread(Runnable runnable) {
			return new Thread(runnable, String.format("FsLogPersister-%d", threadCounter.incrementAndGet()));
		}
	};

	private final ScheduledExecutorService executorService;
	private final LogPathSerializer pathSerializer;
	private final FileSystem fileSystem;
	private final long scheduleDelayMs;
	private final BlockingQueue<LogWriteRequest> requests;
	private final AtomicBoolean queueDrainPending;
	private final Path basePath;
	private volatile Throwable failure;

	// todo low: consider removing requestQueueCapacity argument
	FsLogPersister(Path basePath, int threadPoolSize, long persistDelayMs, int requestQueueCapacity) throws IOException {
		this.basePath = basePath;
		this.fileSystem = basePath.getFileSystem();
		this.scheduleDelayMs = persistDelayMs;
		this.requests = new ArrayBlockingQueue<>(requestQueueCapacity, true); // todo low: use non-fair queue?
		this.executorService = Executors.newScheduledThreadPool(threadPoolSize, THREAD_FACTORY); // todo medium: setup error handler?
		this.queueDrainPending = new AtomicBoolean(false);
		this.pathSerializer = new LogPathSerializer();
	}

	CompletableFuture<LogPointer> persist(UuidLogId logId, List<Tuple2<SequenceNumber, List<LogFragment>>> data) {
		LOG.debug("persist {} sqns for {}", data.size(), logId);
		CompletableFuture<LogPointer> promise = new CompletableFuture<>();
		if (failure != null) {
			promise.completeExceptionally(failure);
			return promise;
		}
		try {
			requests.put(new LogWriteRequest(logId, data, promise));
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		scheduleUploadIfNeeded();
		return promise;
	}

	private void scheduleUploadIfNeeded() {
		if (queueDrainPending.compareAndSet(false, true)) {
			if (scheduleDelayMs > 0) {
				executorService.schedule(this::drainAndExecute, scheduleDelayMs, MILLISECONDS);
			} else {
				executorService.submit(this::drainAndExecute);
			}
		}
	}

	private void drainAndExecute() {
		// todo low: check error?
		ArrayList<LogWriteRequest> batch = new ArrayList<>();
		requests.drainTo(batch);
		try {
			queueDrainPending.set(false); // allow other uploads to be scheduled and run
			if (!requests.isEmpty()) {
				// request was enqueued after drain but probably before flag set to false
				// probably no upload is scheduled
				scheduleUploadIfNeeded();
			}
			executeBatch(batch);
		} catch (Throwable t) {
			for (LogWriteRequest request : batch) {
				request.completeWith(t);
			}
			if (t instanceof IOException) {
				LOG.warn("Caught IO exception while uploading log fragments", t);
			} else {
				// todo low: handle error using executor methods?
				// todo low: shutdown executor?
				failure = t;
				ExceptionUtils.rethrow(t);
			}
		}
	}

	private void executeBatch(List<LogWriteRequest> batch) throws Exception {
		final String fileName = generateFileName();
		LOG.debug("write {} requests to file: {}", batch.size(), fileName);
		try (FSDataOutputStream os = fileSystem.create(new Path(basePath, fileName), NO_OVERWRITE)) { // todo low: inject entropy?
			write(batch, os);
		}
		final byte[] passThroughData = pathSerializer.buildPassThroughData(fileName);
		for (LogWriteRequest request : batch) {
			request.completeWith(passThroughData);
		}
	}

	private void write(List<LogWriteRequest> requests, FSDataOutputStream os) throws IOException {
		// format: logCount, [ logId, [ sqnCount, [ sqn, kgCount, [ kg, byteCount, bytes ] ] ] ]
		LogStreamWriter logStreamWriter = new LogStreamWriter(os);
		logStreamWriter.writeCount(requests.size());
		for (LogWriteRequest request : requests) {
			logStreamWriter.writeLogId(request.logId);
			logStreamWriter.writeCount(request.data.size());
			for (Tuple2<SequenceNumber, List<LogFragment>> e : request.data) {
				logStreamWriter.writeFragments(e.f0, e.f1);
			}
		}
	}

	private String generateFileName() {
		return UUID.randomUUID().toString(); // todo low: review
	}

	private static final class LogWriteRequest {
		private final UuidLogId logId;
		private final List<Tuple2<SequenceNumber, List<LogFragment>>> data;
		private final CompletableFuture<LogPointer> promise;

		private LogWriteRequest(UuidLogId logId, List<Tuple2<SequenceNumber, List<LogFragment>>> data, CompletableFuture<LogPointer> promise) {
			this.logId = logId;
			this.data = data;
			this.promise = promise;
		}

		void completeWith(byte[] passThroughData) {
			promise.complete(LogPointer.of(logId, passThroughData));
		}

		void completeWith(Throwable t) {
			promise.completeExceptionally(t);
		}
	}
}
