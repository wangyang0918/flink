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

package org.apache.flink.yarn.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.deployment.executors.ExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link PipelineExecutor} used when executing jobs in {@code APPLICATION} mode on Yarn.
 * In this mode the user's main method is executed at the job master.
 */
@Internal
public class YarnApplicationExecutor implements PipelineExecutor {

	private static final Logger LOG = LoggerFactory.getLogger(YarnApplicationExecutor.class);

	public static final String NAME = "yarn-application";

	private final CompletableFuture<LeaderGatewayRetriever<DispatcherGateway>> dispatcherRetrieverFuture;

	public YarnApplicationExecutor(final CompletableFuture<LeaderGatewayRetriever<DispatcherGateway>> dispatcherGatewayRetrieverFuture) {
		this.dispatcherRetrieverFuture = checkNotNull(dispatcherGatewayRetrieverFuture);
	}

	@Override
	public CompletableFuture<JobClient> execute(final Pipeline pipeline, final Configuration configuration) {
		final JobGraph jobGraph = ExecutorUtils.getJobGraph(pipeline, configuration);

		final CompletableFuture<Acknowledge> submissionResult = this.dispatcherRetrieverFuture.thenCompose(
				dispatcherRetriever -> dispatcherRetriever.getFuture().thenCompose(
						dispatcher -> dispatcher.submitJob(jobGraph, Time.minutes(5L)))); // TODO: 05.02.20 checkout this timeout

		return CompletableFuture.completedFuture(null);
	}
}
