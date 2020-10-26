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

package org.apache.flink.kubernetes.itcases;

import org.apache.flink.kubernetes.highavailability.KubernetesLeaderElectionDriver;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingContender;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import org.junit.Test;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.kubernetes.highavailability.KubernetesHighAvailabilityTestBase.LEADER_CONFIGMAP_NAME;
import static org.apache.flink.kubernetes.highavailability.KubernetesHighAvailabilityTestBase.LEADER_URL;
import static org.apache.flink.kubernetes.highavailability.KubernetesHighAvailabilityTestBase.createLeaderElectionService;
import static org.apache.flink.kubernetes.highavailability.KubernetesHighAvailabilityTestBase.createLeaderRetrievalService;
import static org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector.LEADER_ANNOTATION_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/**
 * IT Tests for the {@link DefaultLeaderElectionService} with {@link KubernetesLeaderElectionDriver} and
 * {@link DefaultLeaderRetrievalService} with {@link org.apache.flink.kubernetes.highavailability.KubernetesLeaderRetrievalDriver}.
 */
public class KubernetesLeaderElectionITCase extends KubernetesITTestBase {

	/**
	 * Start multiple leaders concurrently, if current leader dies, a new one could take over and update the leader
	 * information successfully. The leader retrieval service should notice this change and notify listener with new
	 * leader address.
	 */
	@Test
	public void testConcurrentLeaderElectionAndRetrieval() throws Exception {
		final String leaderConfigMapName = LEADER_CONFIGMAP_NAME + System.currentTimeMillis();
		final int leaderNum = 3;
		final DefaultLeaderElectionService[] leaderElectionServices = new DefaultLeaderElectionService[leaderNum];
		final ExecutorService[] executorServices = new ExecutorService[leaderNum];
		DefaultLeaderRetrievalService leaderRetrievalService = null;

		try {
			// Start multiple leader contenders
			final TestingContender[] contenders = new TestingContender[leaderNum];
			final String[] lockIdentities = new String[leaderNum];
			for (int i = 0; i < leaderNum; i++) {
				final FlinkKubeClient kubeClientForLeader = kubeClientFactory.fromConfiguration(configuration);
				lockIdentities[i] = UUID.randomUUID().toString();
				executorServices[i] = Executors.newFixedThreadPool(
					4, new ExecutorThreadFactory("IO-Executor-Leader-" + i));
				leaderElectionServices[i] = createLeaderElectionService(
					configuration, kubeClientForLeader, leaderConfigMapName, lockIdentities[i], executorServices[i]);
				contenders[i] = new TestingContender(getLeaderUrl(i), leaderElectionServices[i]);
				leaderElectionServices[i].start(contenders[i]);
			}

			// Start the leader retrieval
			leaderRetrievalService = createLeaderRetrievalService(flinkKubeClient, leaderConfigMapName);
			final TestingListener listener = new TestingListener();
			leaderRetrievalService.start(listener);

			final String currentLeaderAddress = listener.waitForNewLeader(TIMEOUT);
			final String currentLeaderSessionId = listener.getLeaderSessionID().toString();
			final Optional<KubernetesConfigMap> configMapOpt = flinkKubeClient.getConfigMap(leaderConfigMapName);
			assertThat(configMapOpt.isPresent(), is(true));
			final String currentLock = configMapOpt.get().getAnnotations().get(LEADER_ANNOTATION_KEY);
			for (int i = 0; i < 3; i++) {
				if (getLeaderUrl(i).equals(currentLeaderAddress)) {
					assertThat(currentLock, containsString(lockIdentities[i]));
					contenders[i].waitForLeader(TIMEOUT);
					assertThat(currentLeaderAddress, is(contenders[i].getDescription()));
					assertThat(currentLeaderSessionId, is(contenders[i].getLeaderSessionID().toString()));
					// Stop the current leader
					leaderElectionServices[i].stop();
					executorServices[i].shutdownNow();
					break;
				}
			}

			// Another leader should elected successfully and is granted leadership
			final String newLeaderAddress = listener.waitForNewLeader(TIMEOUT);
			assertThat(newLeaderAddress, is(not(currentLeaderAddress)));
		} finally {
			// Cleanup the resources
			for (int i = 0; i < leaderNum; i++) {
				if (leaderElectionServices[i] != null) {
					leaderElectionServices[i].stop();
				}
				if (executorServices[i] != null) {
					executorServices[i].shutdownNow();
				}
			}
			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}
			flinkKubeClient.deleteConfigMap(leaderConfigMapName).get();
		}
	}

	private String getLeaderUrl(int i) {
		return LEADER_URL + "_" + i;
	}
}
