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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.StreamOperator;

import javax.annotation.Nonnull;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class handles the close, endInput and other related logic of a {@link StreamOperator}.
 * It also automatically propagates the close operation to the next wrapper that the {@link #next}
 * points to, so we can use {@link #next} to link all operator wrappers in the operator chain and
 * close all operators only by calling the {@link #close(StreamTaskActionExecutor)} method of the
 * header operator wrapper.
 */
@Internal
public class StreamOperatorWrapper<OUT, OP extends StreamOperator<OUT>> {

	private final OP wrapped;

	private final MailboxExecutor mailboxExecutor;

	private StreamOperatorWrapper<?, ?> previous;

	private StreamOperatorWrapper<?, ?> next;

	StreamOperatorWrapper(OP wrapped, MailboxExecutor mailboxExecutor) {
		this.wrapped = checkNotNull(wrapped);
		this.mailboxExecutor = checkNotNull(mailboxExecutor);
	}

	/**
	 * Closes the wrapped operator and propagates the close operation to the next wrapper that the
	 * {@link #next} points to.
	 *
	 * <p>Note that this method must be called in the task thread, because we need to call
	 * {@link MailboxExecutor#yield()} to take the mails of closing operator and running timers and
	 * run them.
	 */
	public void close(StreamTaskActionExecutor actionExecutor) throws Exception {
		close(actionExecutor, false);
	}

	/**
	 * Ends an input of the operator contained by this wrapper.
	 *
	 * @param inputId the input ID starts from 1 which indicates the first input.
	 */
	public void endOperatorInput(int inputId) throws Exception {
		if (wrapped instanceof BoundedOneInput) {
			((BoundedOneInput) wrapped).endInput();
		} else if (wrapped instanceof BoundedMultiInput) {
			((BoundedMultiInput) wrapped).endInput(inputId);
		}
	}

	public OP getStreamOperator() {
		return wrapped;
	}

	void setPrevious(StreamOperatorWrapper previous) {
		this.previous = previous;
	}

	void setNext(StreamOperatorWrapper next) {
		this.next = next;
	}

	private void close(StreamTaskActionExecutor actionExecutor, boolean invokingEndInput) throws Exception {
		if (invokingEndInput) {
			// NOTE: This only do for the case where the operator is one-input operator. At present,
			// any non-head operator on the operator chain is one-input operator.
			actionExecutor.runThrowing(() -> endOperatorInput(1));
		}

		closeOperatorAndQuiesceTimeService(actionExecutor);

		// propagate the close operation to the next wrapper
		if (next != null) {
			next.close(actionExecutor, true);
		}
	}

	private void closeOperatorAndQuiesceTimeService(StreamTaskActionExecutor actionExecutor)
		throws InterruptedException, ExecutionException {

		final CompletableFuture<Void> closedFuture = new CompletableFuture<>();

		// 1. executing the close operation must be deferred to the mailbox to ensure that mails already
		//    in the mailbox are finished before closing the operator
		// 2. to ensure that there is no longer output triggered by the timers before invoke the "endInput"
		//    methods of downstream operators in the operator chain, we must quiesce the processing time
		//    service to prevent the pending timers from firing, but allow the timers in running to finish
		// 3. when the second step is finished, send a closed mail to ensure that the timers yet in mailbox
		//    are finished before exiting the following mailbox processing loop
		// TODO: To ensure the strict semantics of "close", the operator should be allowed to decide how to
		//  handle (cancel or trigger) the pending timers before being closed, and the second step should be
		//  finished before the first step
		deferCloseOperatorToMailbox(actionExecutor)
			.thenCompose(unused  -> quiesceProcessingTimeService())
			.thenRun(() -> sendClosedMail(closedFuture))
			.exceptionally((throwable) -> forwardException(throwable, closedFuture));

		// run the mailbox processing loop until the closing operation is finished
		while (!closedFuture.isDone()) {
			mailboxExecutor.yield();
		}

		// expose the exception thrown when closing
		if (closedFuture.isCompletedExceptionally()) {
			closedFuture.get();
		}
	}

	private CompletableFuture<Void> deferCloseOperatorToMailbox(StreamTaskActionExecutor actionExecutor) {
		final CompletableFuture<Void> closeOperatorFuture = new CompletableFuture<>();

		mailboxExecutor.execute(
			() -> {
				try {
					closeOperator(actionExecutor);
					closeOperatorFuture.complete(null);
				} catch (Throwable t) {
					closeOperatorFuture.completeExceptionally(t);
				}
			},
			"StreamOperatorWrapper#closeOperator for " + wrapped
		);
		return closeOperatorFuture;
	}

	private CompletableFuture<Void> quiesceProcessingTimeService() {
		if (wrapped instanceof ProcessingTimeServiceAware) {
			ProcessingTimeService processingTimeService = ((ProcessingTimeServiceAware) wrapped).getProcessingTimeService();
			if (processingTimeService != null) {
				return processingTimeService.quiesce();
			}
		}
		return CompletableFuture.completedFuture(null);
	}

	private void sendClosedMail(CompletableFuture<Void> closedFuture) {
		mailboxExecutor.execute(() ->
			closedFuture.complete(null), "StreamOperatorWrapper#sendClosedMail for " + wrapped);
	}

	private Void forwardException(Throwable throwable, CompletableFuture<?> future) {
		if (throwable != null) {
			future.completeExceptionally(throwable);
		}
		return null;
	}

	private void closeOperator(StreamTaskActionExecutor actionExecutor) throws Exception {
		actionExecutor.runThrowing(wrapped::close);
	}

	static class ReadIterator implements Iterator<StreamOperatorWrapper<?, ?>>, Iterable<StreamOperatorWrapper<?, ?>> {

		private final boolean reverse;

		private StreamOperatorWrapper<?, ?> current;

		ReadIterator(StreamOperatorWrapper<?, ?> first, boolean reverse) {
			this.current = first;
			this.reverse = reverse;
		}

		@Override
		public boolean hasNext() {
			return this.current != null;
		}

		@Override
		public StreamOperatorWrapper<?, ?> next() {
			if (hasNext()) {
				StreamOperatorWrapper<?, ?> next = current;
				current = reverse ? current.previous : current.next;
				return next;
			}

			throw new NoSuchElementException();
		}

		@Nonnull
		@Override
		public Iterator<StreamOperatorWrapper<?, ?>> iterator() {
			return this;
		}
	}
}
