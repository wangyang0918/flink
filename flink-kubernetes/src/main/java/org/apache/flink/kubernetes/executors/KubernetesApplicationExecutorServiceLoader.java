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

package org.apache.flink.kubernetes.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link PipelineExecutorServiceLoader} that returns ONLY an executor
 * factory of type {@link KubernetesApplicationExecutorFactory}.
 *
 * <p>This is used when executing jobs in {@code APPLICATION} mode where
 * the user's main method is executed at the job master.
 */
@Internal
public class KubernetesApplicationExecutorServiceLoader implements PipelineExecutorServiceLoader {

	private final CompletableFuture<LeaderGatewayRetriever<DispatcherGateway>> dispatcherGatewayRetrieverFuture;

	public KubernetesApplicationExecutorServiceLoader(
			final CompletableFuture<LeaderGatewayRetriever<DispatcherGateway>> dispatcherGatewayRetrieverFuture) {
		this.dispatcherGatewayRetrieverFuture = checkNotNull(dispatcherGatewayRetrieverFuture);
	}

	@Override
	public PipelineExecutorFactory getExecutorFactory(final Configuration configuration) {
		return new KubernetesApplicationExecutorFactory(dispatcherGatewayRetrieverFuture);
	}

	@Override
	public Stream<String> getExecutorNames() {
		return Stream.<String>builder().add(KubernetesApplicationExecutor.NAME).build();
	}
}
