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

package org.apache.flink.kubernetes;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.AbstractClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.executors.KubernetesSessionClusterExecutor;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;

import javax.annotation.Nullable;

import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ClusterClientFactory} for a Kubernetes cluster.
 */
@Internal
public class KubernetesClusterClientFactory extends AbstractClusterClientFactory<String> {

	private static final String CLUSTER_ID_PREFIX = "flink-cluster-";

	@Override
	public boolean isCompatibleWith(Configuration configuration) {
		checkNotNull(configuration);
		final String deploymentTarget = configuration.getString(DeploymentOptions.TARGET);
		return KubernetesSessionClusterExecutor.NAME.equalsIgnoreCase(deploymentTarget);
	}

	@Override
	public KubernetesClusterDescriptor createClusterDescriptor(Configuration configuration) {
		checkNotNull(configuration);
		String clusterId = configuration.getString(KubernetesConfigOptions.CLUSTER_ID);
		if (clusterId == null) {
			clusterId = generateClusterId();
			configuration.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId);
		}
		return new KubernetesClusterDescriptor(configuration, KubeClientFactory.fromConfiguration(configuration));
	}

	@Nullable
	@Override
	public String getClusterId(Configuration configuration) {
		checkNotNull(configuration);
		return configuration.getString(KubernetesConfigOptions.CLUSTER_ID);
	}

	private String generateClusterId() {
		return CLUSTER_ID_PREFIX + UUID.randomUUID();
	}
}
