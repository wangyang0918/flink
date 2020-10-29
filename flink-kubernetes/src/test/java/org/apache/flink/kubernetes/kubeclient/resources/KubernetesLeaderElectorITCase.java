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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.KubernetesResource;
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.DefaultKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.highavailability.KubernetesHighAvailabilityTestBase.LEADER_CONFIGMAP_NAME;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/**
 * IT Tests for the {@link KubernetesLeaderElector}. Start multiple leader contenders currently, one should elect
 * successfully. And  if current leader dies, a new one could take over.
 */
public class KubernetesLeaderElectorITCase {

	@ClassRule
	public static KubernetesResource kubernetesResource = new KubernetesResource();

	private static final long TIMEOUT = 120L * 1000L;

	private final KubeClientFactory kubeClientFactory = new DefaultKubeClientFactory();

	@Test
	public void testMultipleKubernetesLeaderElectors() throws Exception {
		final Configuration configuration = kubernetesResource.getConfiguration();
		final ExecutorService executorService = kubernetesResource.getExecutorService();

		final BlockingQueue<String> leaderStore = new LinkedBlockingQueue<>();
		final String leaderConfigMapName = LEADER_CONFIGMAP_NAME + System.currentTimeMillis();
		final int leaderNum = 3;

		final KubernetesLeaderElector[] leaderElectors = new KubernetesLeaderElector[leaderNum];
		// We use different Kubernetes clients for different leader electors.
		final FlinkKubeClient[] kubeClients = new FlinkKubeClient[leaderNum];
		final TestingLeaderCallbackHandler[] leaderCallbackHandlers = new TestingLeaderCallbackHandler[leaderNum];

		try {
			for (int i = 0; i < leaderNum; i++) {
				kubeClients[i] = kubeClientFactory.fromConfiguration(configuration, executorService);
				leaderCallbackHandlers[i] = new TestingLeaderCallbackHandler(leaderStore, UUID.randomUUID().toString());
				final KubernetesLeaderElectionConfiguration leaderConfig = new KubernetesLeaderElectionConfiguration(
					leaderConfigMapName, leaderCallbackHandlers[i].getLockIdentity(), configuration);
				leaderElectors[i] = kubeClients[i].createLeaderElector(leaderConfig, leaderCallbackHandlers[i]);

				// Start the leader electors to contending the leader
				leaderElectors[i].run();
			}

			// Wait for the first leader
			final String firstLockIdentity = leaderStore.poll(TIMEOUT, TimeUnit.MILLISECONDS);

			for (int i = 0; i < leaderNum; i++) {
				if (leaderCallbackHandlers[i].getLockIdentity().equals(firstLockIdentity)) {
					// Check the callback is called.
					assertThat(leaderCallbackHandlers[i].hasLeadership(), is(true));
					// Current leader died
					leaderElectors[i].stop();
				} else {
					assertThat(leaderCallbackHandlers[i].hasLeadership(), is(false));
				}
			}

			// Another leader should elected successfully and update the lock identity
			final String anotherLockIdentity = leaderStore.poll(TIMEOUT, TimeUnit.MILLISECONDS);
			assertThat(anotherLockIdentity, is(not(firstLockIdentity)));
		} finally {
			// Cleanup the resources
			for (int i = 0; i < leaderNum; i++) {
				if (leaderElectors[i] != null) {
					leaderElectors[i].stop();
				}
				if (kubeClients[i] != null) {
					kubeClients[i].close();
				}
			}
			kubernetesResource.getFlinkKubeClient().deleteConfigMap(leaderConfigMapName).get();
		}
	}
}
