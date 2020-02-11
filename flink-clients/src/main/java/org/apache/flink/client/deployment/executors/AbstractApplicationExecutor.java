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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link PipelineExecutor} used when executing jobs in {@code APPLICATION} mode on Yarn.
 * In this mode the user's main method is executed at the job master.
 */
@Internal
public class AbstractApplicationExecutor implements PipelineExecutor {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractApplicationExecutor.class);

	private final CompletableFuture<LeaderGatewayRetriever<DispatcherGateway>> dispatcherRetrieverFuture;

	public AbstractApplicationExecutor(final CompletableFuture<LeaderGatewayRetriever<DispatcherGateway>> dispatcherGatewayRetrieverFuture) {
		this.dispatcherRetrieverFuture = checkNotNull(dispatcherGatewayRetrieverFuture);
	}

	@Override
	public CompletableFuture<JobClient> execute(final Pipeline pipeline, final Configuration configuration) {
		checkNotNull(pipeline);
		checkNotNull(configuration);

		final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(configuration);
		final List<URL> classpathURLs = configAccessor.getClasspaths();
		final List<URL> jarURLs = configAccessor.getJars();
		final SavepointRestoreSettings savepointRestoreSettings = configAccessor.getSavepointRestoreSettings();

		final JobGraph jobGraph = ExecutorUtils.getJobGraph(pipeline, configuration);
		jobGraph.addJars(jarURLs);
		jobGraph.setClasspaths(classpathURLs);
		jobGraph.setSavepointRestoreSettings(savepointRestoreSettings);

		final JobID jobID = jobGraph.getJobID();

		LOG.info("Executing job {} with configuration= {}.", jobID, configuration);

		final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));
		return submitGraphFiles(configuration, jobGraph, timeout)
				.thenApply(ack -> new GatewayRetrieverJobClientAdapter(dispatcherRetrieverFuture, jobID, timeout));
	}

	private CompletableFuture<Acknowledge> submitGraphFiles(
			final Configuration configuration,
			final JobGraph jobGraph,
			final Time timeout) {

		checkNotNull(jobGraph);
		checkNotNull(timeout);

		final CompletableFuture<DispatcherGateway> gatewayRetrieverFuture =
				dispatcherRetrieverFuture.thenCompose(LeaderGatewayRetriever::getFuture);

		return gatewayRetrieverFuture.thenCompose(dispatcher ->
				dispatcher.getBlobServerPort(timeout).thenCompose(blobServerPort -> {
					final InetSocketAddress address = new InetSocketAddress(dispatcher.getHostname(), blobServerPort);

					final JobGraph updatedJobGraph = uploadJobGraphFiles(configuration, address, jobGraph);
					return dispatcher.submitJob(updatedJobGraph, timeout);
				}));
	}

	private JobGraph uploadJobGraphFiles(
			final Configuration configuration,
			final InetSocketAddress blobServerAddress,
			final JobGraph jobGraph) {
		try {
			ClientUtils.extractAndUploadJobGraphFiles(jobGraph, () -> new BlobClient(blobServerAddress, configuration));
		} catch (FlinkException e) {
			throw new CompletionException(e);
		}
		return jobGraph;
	}
}
