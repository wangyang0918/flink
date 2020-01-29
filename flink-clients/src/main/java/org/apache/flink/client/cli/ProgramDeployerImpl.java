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

package org.apache.flink.client.cli;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The default implementation of a {@link ProgramDeployer}.
 */
@Internal
public class ProgramDeployerImpl implements ProgramDeployer {

	private final ClusterClientServiceLoader clientServiceLoader;

	public ProgramDeployerImpl(final ClusterClientServiceLoader clientServiceLoader) {
		this.clientServiceLoader = checkNotNull(clientServiceLoader);
	}

	@Override
	public <ClusterID> void deploy(final Configuration configuration) throws Exception {
		final ClusterClientFactory<ClusterID> clientFactory = clientServiceLoader.getClusterClientFactory(configuration);
		try (final ClusterDescriptor<ClusterID> clusterDescriptor = clientFactory.createClusterDescriptor(configuration)) {
			final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(configuration);

			// TODO: 29.01.20 the following call should change to return a different, application client.
			final ClusterClientProvider<ClusterID> clusterClientProvider = clusterDescriptor.deployApplicationCluster(clusterSpecification);
		}
	}
}
