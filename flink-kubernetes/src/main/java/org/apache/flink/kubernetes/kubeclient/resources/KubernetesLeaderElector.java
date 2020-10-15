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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.kubernetes.kubeclient.KubernetesLeaderElectionConfiguration;

import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElector;
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.ConfigMapLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.kubernetes.utils.Constants.LOCK_IDENTITY;

/**
 * Represent Leader Elector in kubernetes.
 */
public class KubernetesLeaderElector extends LeaderElector<NamespacedKubernetesClient> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesLeaderElector.class);

	public KubernetesLeaderElector(
			NamespacedKubernetesClient kubernetesClient,
			String namespace,
			KubernetesLeaderElectionConfiguration leaderConfig,
			LeaderCallbackHandler leaderCallbackHandler) {
		super(kubernetesClient, new LeaderElectionConfigBuilder()
			.withName(leaderConfig.getConfigMapName())
			.withLeaseDuration(leaderConfig.getLeaseDuration())
			.withLock(new ConfigMapLock(namespace, leaderConfig.getConfigMapName(), LOCK_IDENTITY))
			.withRenewDeadline(leaderConfig.getRenewDeadline())
			.withRetryPeriod(leaderConfig.getRetryPeriod())
			.withLeaderCallbacks(new LeaderCallbacks(
				leaderCallbackHandler::isLeader,
				leaderCallbackHandler::notLeader,
				newLeader -> LOG.info("New leader elected {}.", newLeader)
			))
			.build());
		LOG.info("Create KubernetesLeaderElector {} with lock identity {}.",
			leaderConfig.getConfigMapName(), LOCK_IDENTITY);
	}

	/**
	 * Callback handler for leader election.
	 */
	public abstract static class LeaderCallbackHandler {

		public abstract void isLeader();

		public abstract void notLeader();
	}
}
