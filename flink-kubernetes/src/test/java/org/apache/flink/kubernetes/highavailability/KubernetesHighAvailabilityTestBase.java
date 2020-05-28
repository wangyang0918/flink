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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.TestingFlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.runtime.leaderelection.TestingContender;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link KubernetesLeaderElectionService}.
 */
public class KubernetesHighAvailabilityTestBase extends TestLogger {

	private final ExecutorService executorService =
		Executors.newFixedThreadPool(4, new ExecutorThreadFactory("IO-Executor"));
	private final Configuration configuration = new Configuration();

	protected static final String CLUSTER_ID = "leader-test-cluster";
	protected static final String LEADER_URL = "akka.tcp://flink@172.20.1.21:6123/user/rpc/resourcemanager";
	protected static final long TIMEOUT = 30L * 1000L;
	protected static final String LEADER_CONFIGMAP_NAME = "k8s-ha-app1-resourcemanager";
	protected final Map<String, KubernetesConfigMap> configMapStore = new HashMap<>();
	protected final CompletableFuture<FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>> configMapsAndDoCallbackFuture =
		new CompletableFuture<>();
	protected final CompletableFuture<FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>> leaderRetrievalConfigMapCallback =
		new CompletableFuture<>();

	@Before
	public void setup() {
		configuration.setString(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
	}

	@After
	public void teardown() {
		executorService.shutdownNow();
	}

	protected KubernetesLeaderElectionService createLeaderElectionService(AtomicBoolean leaderController) {
		final TestingFlinkKubeClient flinkKubeClient = TestingFlinkKubeClient.builder()
			.setConfigMapStore(configMapStore)
			.setWatchConfigMapsAndDoCallbackFunction((ignore, handler) -> {
				configMapsAndDoCallbackFuture.complete(handler);
				return new TestingFlinkKubeClient.MockKubernetesWatch();
			})
			.setLeaderController(leaderController).build();
		flinkKubeClient.createConfigMap(LEADER_CONFIGMAP_NAME, new HashMap<>(), LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY);
		return new KubernetesLeaderElectionService(
			flinkKubeClient,
			executorService,
			KubernetesLeaderElectionConfiguration.fromConfiguration(LEADER_CONFIGMAP_NAME, configuration));
	}

	protected KubernetesLeaderRetrievalService createLeaderRetrievalService() {
		final TestingFlinkKubeClient flinkKubeClient = TestingFlinkKubeClient.builder()
			.setConfigMapStore(configMapStore)
			.setWatchConfigMapsAndDoCallbackFunction((ignore, handler) -> {
				leaderRetrievalConfigMapCallback.complete(handler);
				return new TestingFlinkKubeClient.MockKubernetesWatch();
			}).build();
		return new KubernetesLeaderRetrievalService(flinkKubeClient, LEADER_CONFIGMAP_NAME);
	}

	/**
	 * Context to leader election and retrieval tests.
	 */
	protected class Context {
		final AtomicBoolean leaderController = new AtomicBoolean(false);
		final KubernetesLeaderElectionService leaderElectionService = createLeaderElectionService(leaderController);
		final TestingContender contender = new TestingContender(LEADER_URL, leaderElectionService);

		final KubernetesLeaderRetrievalService leaderRetrievalService = createLeaderRetrievalService();
		final TestingListener listener = new TestingListener();

		protected final void runTest(RunnableWithException testMethod) throws Exception {
			leaderElectionService.start(contender);
			leaderController.set(true);
			contender.waitForLeader(TIMEOUT);
			assertThat(contender.isLeader(), is(true));
			leaderRetrievalService.start(listener);
			testMethod.run();
			leaderElectionService.stop();
			leaderRetrievalService.stop();
		}

		protected KubernetesConfigMap getLeaderConfigMap() {
			assertThat(configMapStore.get(LEADER_CONFIGMAP_NAME), is(notNullValue()));
			return configMapStore.get(LEADER_CONFIGMAP_NAME);
		}
	}
}
