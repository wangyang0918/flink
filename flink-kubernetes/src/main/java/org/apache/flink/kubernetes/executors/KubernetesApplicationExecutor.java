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
import org.apache.flink.client.deployment.executors.AbstractApplicationExecutor;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import java.util.concurrent.CompletableFuture;

/**
 * A {@link PipelineExecutor} used when executing jobs in {@code APPLICATION} mode on Yarn.
 * In this mode the user's main method is executed at the job master.
 */
@Internal
public class KubernetesApplicationExecutor extends AbstractApplicationExecutor {

	public static final String NAME = "kubernetes-application";

	public KubernetesApplicationExecutor(CompletableFuture<LeaderGatewayRetriever<DispatcherGateway>> dispatcherGatewayRetrieverFuture) {
		super(dispatcherGatewayRetrieverFuture);
	}
}
