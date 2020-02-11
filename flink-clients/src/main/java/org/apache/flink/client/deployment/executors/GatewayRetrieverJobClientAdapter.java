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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Javadoc.
 */
@Internal
public class GatewayRetrieverJobClientAdapter implements JobClient {

	private final Time timeout;

	private final JobID jobID;

	private final CompletableFuture<LeaderGatewayRetriever<DispatcherGateway>> dispatcherGatewayRetrieverFuture;

	public GatewayRetrieverJobClientAdapter(
			final CompletableFuture<LeaderGatewayRetriever<DispatcherGateway>> dispatcherGatewayRetrieverFuture,
			final JobID jobID,
			final Time timeout) {
		this.dispatcherGatewayRetrieverFuture = checkNotNull(dispatcherGatewayRetrieverFuture);
		this.jobID = checkNotNull(jobID);
		this.timeout = checkNotNull(timeout);
	}

	@Override
	public JobID getJobID() {
		return jobID;
	}

	@Override
	public CompletableFuture<JobStatus> getJobStatus() {
		return dispatcherGatewayRetrieverFuture.thenCompose(dispatcherRetriever ->
				dispatcherRetriever.getFuture().thenCompose(dispatcher ->
						dispatcher.requestJobStatus(jobID, timeout)));
	}

	@Override
	public CompletableFuture<Void> cancel() {
		return dispatcherGatewayRetrieverFuture.thenCompose(dispatcherRetriever ->
				dispatcherRetriever.getFuture().thenCompose(dispatcher ->
						dispatcher.cancelJob(jobID, timeout)))
				.thenApply(ack -> null);
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(boolean advanceToEndOfEventTime, @Nullable String savepointDirectory) {
		return dispatcherGatewayRetrieverFuture.thenCompose(dispatcherRetriever ->
				dispatcherRetriever.getFuture().thenCompose(dispatcher ->
						dispatcher.stopWithSavepoint(jobID, savepointDirectory, advanceToEndOfEventTime, timeout)));
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory) {
		return dispatcherGatewayRetrieverFuture.thenCompose(dispatcherRetriever ->
				dispatcherRetriever.getFuture().thenCompose(dispatcher ->
						dispatcher.triggerSavepoint(jobID, savepointDirectory, false, timeout)));
	}

	@Override
	public CompletableFuture<Map<String, Object>> getAccumulators(ClassLoader classLoader) {
		// TODO: 06.02.20 fix before opening PR.
		throw new UnsupportedOperationException("Not supported yet by the dispatcher.");
	}

	@Override
	public CompletableFuture<JobExecutionResult> getJobExecutionResult(ClassLoader userClassloader) {
		checkNotNull(userClassloader);
		return dispatcherGatewayRetrieverFuture.thenCompose(dispatcherRetriever ->
				dispatcherRetriever.getFuture().thenCompose(dispatcher ->
						dispatcher.requestJobResult(jobID, timeout)))
				.thenApply(result -> {
					try {
						return result.toJobExecutionResult(userClassloader);
					} catch (Throwable t) {
						throw new CompletionException(new ProgramInvocationException("Job failed", jobID, t));
					}
				});
	}
}
