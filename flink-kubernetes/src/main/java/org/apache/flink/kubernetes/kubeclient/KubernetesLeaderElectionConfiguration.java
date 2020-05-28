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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesHighAvailabilityOptions;

import java.time.Duration;

/**
 * Configuration specific to {@link org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector}.
 */
public class KubernetesLeaderElectionConfiguration {

	private final String clusterId;
	private final String configMapName;
	private final Duration leaseDuration;
	private final Duration renewDeadline;
	private final Duration retryPeriod;

	private KubernetesLeaderElectionConfiguration(
			String clusterId,
			String configMapName,
			Duration leaseDuration,
			Duration renewDeadline,
			Duration retryPeriod) {
		this.clusterId = clusterId;
		this.configMapName = configMapName;
		this.leaseDuration = leaseDuration;
		this.renewDeadline = renewDeadline;
		this.retryPeriod = retryPeriod;
	}

	public String getClusterId() {
		return clusterId;
	}

	public String getConfigMapName() {
		return configMapName;
	}

	public Duration getLeaseDuration() {
		return leaseDuration;
	}

	public Duration getRenewDeadline() {
		return renewDeadline;
	}

	public Duration getRetryPeriod() {
		return retryPeriod;
	}

	public static KubernetesLeaderElectionConfiguration fromConfiguration(String configMapName, Configuration config) {
		final String clusterId = config.getString(KubernetesConfigOptions.CLUSTER_ID);

		final Duration leaseDuration = config.get(KubernetesHighAvailabilityOptions.KUBERNETES_LEASE_DURATION);
		final Duration renewDeadline = config.get(KubernetesHighAvailabilityOptions.KUBERNETES_RENEW_DEADLINE);
		final Duration retryPeriod = config.get(KubernetesHighAvailabilityOptions.KUBERNETES_RETRY_PERIOD);

		return new KubernetesLeaderElectionConfiguration(
			clusterId, configMapName, leaseDuration, renewDeadline, retryPeriod);
	}
}
