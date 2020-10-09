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
import java.nio.charset.StandardCharsets;
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
	private static final Logger LOG = LoggerFactory.getLogger(FsLogPersister.class); // todo: log
	private static final ThreadFactory THREAD_FACTORY = new ThreadFactory() {
		private final AtomicInteger threadCounter = new AtomicInteger();

		@Override
		public Thread newThread(Runnable runnable) {
			return new Thread(runnable, String.format("FsLogPersister-%d", threadCounter.incrementAndGet()));
		}
	};

	private final ScheduledExecutorService executorService;
	private final FileSystem fileSystem;
	private final long scheduleDelayMs;
	private final BlockingQueue<UploadRequest> requests;
	private final AtomicBoolean queueDrainPending;
	private volatile Throwable failure;

	// todo: consider removing requestQueueCapacity argument
	FsLogPersister(Path basePath, int threadPoolSize, long persistDelayMs, int requestQueueCapacity) throws IOException {
		fileSystem = basePath.getFileSystem();
		scheduleDelayMs = persistDelayMs;
		requests = new ArrayBlockingQueue<>(requestQueueCapacity, true); // todo: use non-fair queue?
		executorService = Executors.newScheduledThreadPool(threadPoolSize, THREAD_FACTORY); // todo: setup error handler?
		queueDrainPending = new AtomicBoolean(false);
	}

	CompletableFuture<LogPointer> persist(UuidLogId logId, List<Tuple2<SequenceNumber, List<LogFragment>>> data) {
		CompletableFuture<LogPointer> promise = new CompletableFuture<>();
		if (failure != null) {
			promise.completeExceptionally(failure);
			return promise;
		}
		try {
			requests.put(new UploadRequest(logId, data, promise));
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		scheduleUploadIfNeeded();
		return promise;
	}

	private void scheduleUploadIfNeeded() {
		if (queueDrainPending.compareAndSet(false, true)) {
			executorService.schedule(this::drainAndExecute, scheduleDelayMs, MILLISECONDS);
		}
	}

	private void drainAndExecute() {
		// todo: check error?
		ArrayList<UploadRequest> work = new ArrayList<>();
		requests.drainTo(work);
		try {
			queueDrainPending.set(false); // allow other uploads to be scheduled and run
			if (!requests.isEmpty()) {
				// request was enqueued after drain but probably before flag set to false
				// probably no upload is scheduled
				scheduleUploadIfNeeded();
			}
			execute(work);
		} catch (Throwable t) {
			for (UploadRequest request : work) {
				request.completeWith(t);
			}
			if (t instanceof IOException) {
				LOG.warn("Caught IO exception while uploading log fragments", t);
			} else {
				// todo: handle error using executor methods?
				// todo: shutdown executor?
				failure = t;
				ExceptionUtils.rethrow(t);
			}
		}
	}

	private void execute(List<UploadRequest> requests) throws IOException {
		final String fileName = generateFileName();
		try (FSDataOutputStream os = fileSystem.create(new Path(fileName), NO_OVERWRITE)) { // todo: inject entropy?
			write(requests, os);
		}
		final byte[] passThroughData = buildPassThroughData(fileName);
		for (UploadRequest request : requests) {
			request.completeWith(passThroughData);
		}
	}

	private void write(List<UploadRequest> requests, FSDataOutputStream os) throws IOException {
		// todo: buffering?
		LogFragmentWriter logFragmentWriter = new LogFragmentWriter(os);
		for (UploadRequest request : requests) {
			logFragmentWriter.writeLogId(request.logId);
			for (Tuple2<SequenceNumber, List<LogFragment>> e : request.data) {
				logFragmentWriter.writeFragments(e.f0, e.f1);
			}
		}
	}

	private byte[] buildPassThroughData(String fileName) {
		// todo: include home path?
		return fileName.getBytes(StandardCharsets.UTF_8); // todo: review
	}

	private String generateFileName() {
		return UUID.randomUUID().toString(); // todo: review
	}

	private static final class UploadRequest {
		private final UuidLogId logId;
		private final List<Tuple2<SequenceNumber, List<LogFragment>>> data;
		private final CompletableFuture<LogPointer> promise;

		private UploadRequest(UuidLogId logId, List<Tuple2<SequenceNumber, List<LogFragment>>> data, CompletableFuture<LogPointer> promise) {
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
