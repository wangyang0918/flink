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
import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.TestingFlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderElectionDriver;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionEventHandler;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalEventHandler;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriver;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Base class for high availability unit tests with a configured testing Kubernetes client.
 */
public class KubernetesHighAvailabilityTestBase extends TestLogger {
	private static final String CLUSTER_ID = "leader-test-cluster";

	public static final String LOCK_IDENTITY = UUID.randomUUID().toString();
	public static final String LEADER_URL = "akka.tcp://flink@172.20.1.21:6123/user/rpc/dispatcher";
	public static final String LEADER_CONFIGMAP_NAME = "leader-test-cluster";

	protected static final long TIMEOUT = 30L * 1000L;

	protected ExecutorService executorService;
	protected Configuration configuration;

	@Before
	public void setup() {
		configuration = new Configuration();
		configuration.setString(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
		executorService = Executors.newFixedThreadPool(4, new ExecutorThreadFactory("IO-Executor"));
	}

	@After
	public void teardown() throws Exception {
		executorService.shutdownNow();
		executorService.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
	}

	/**
	 * Context to leader election and retrieval tests.
	 */
	protected class Context {
		/**
		 * The configMapStore should only be used for set the following functions. Use a ConcurrentHashMap here since it
		 * could be updated by {@link TestingFlinkKubeClient.MockKubernetesLeaderElector} and
		 * {@link KubernetesLeaderElectionDriver}.
		 */
		private final Map<String, KubernetesConfigMap> configMapStore = new ConcurrentHashMap<>();

		final AtomicBoolean leaderController = new AtomicBoolean(false);

		final List<CompletableFuture<FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>>>
			configMapCallbackFutures = new ArrayList<>();

		final CompletableFuture<Map<String, String>> deleteConfigMapByLabelsFuture = new CompletableFuture<>();
		final CompletableFuture<Void> closeKubeClientFuture = new CompletableFuture<>();

		final FlinkKubeClient flinkKubeClient;

		final LeaderElectionDriver leaderElectionDriver;
		final TestingLeaderElectionEventHandler electionEventHandler;

		final LeaderRetrievalDriver leaderRetrievalDriver;
		final TestingLeaderRetrievalEventHandler retrievalEventHandler;

		Context() {
			flinkKubeClient = getFlinkKubeClient();

			electionEventHandler = new TestingLeaderElectionEventHandler(LEADER_URL);
			leaderElectionDriver = createLeaderElectionDriver();

			retrievalEventHandler = new TestingLeaderRetrievalEventHandler();
			leaderRetrievalDriver = createLeaderRetrievalDriver();
		}

		void runTestAndGrantLeadershipToContender(RunnableWithException testMethod) throws Exception {
			// Grant leadership
			leaderController.set(true);
			electionEventHandler.waitForLeader(TIMEOUT);
			assertThat(electionEventHandler.isLeader(), is(true));

			testMethod.run();

			leaderElectionDriver.close();
			leaderRetrievalDriver.close();
		}

		KubernetesConfigMap getLeaderConfigMap() {
			final Optional<KubernetesConfigMap> configMapOpt = flinkKubeClient.getConfigMap(LEADER_CONFIGMAP_NAME);
			assertThat(configMapOpt.isPresent(), is(true));
			return configMapOpt.get();
		}

		protected FlinkKubeClient getFlinkKubeClient() {
			return TestingFlinkKubeClient.builder()
				.setCreateConfigMapFunction(configMap -> {
					configMapStore.put(configMap.getName(), configMap);
					return CompletableFuture.completedFuture(null);
				})
				.setGetConfigMapFunction(configMapName -> Optional.ofNullable(configMapStore.get(configMapName)))
				.setCheckAndUpdateConfigMapFunction((configMapName, updateFunction) -> {
					final KubernetesConfigMap configMap = configMapStore.get(configMapName);
					if (configMap != null) {
						try {
							final boolean updated = updateFunction.apply(configMap).map(
								updateConfigMap -> {
									configMapStore.put(configMap.getName(), updateConfigMap);
									return true;
								}).orElse(false);
							return CompletableFuture.completedFuture(updated);
						} catch (Throwable throwable) {
							throw new CompletionException(new KubernetesException("Failed to execute update fuction"));
						}
					}
					throw new CompletionException(
						new KubernetesException("ConfigMap " + configMapName + " does not exists."));
				})
				.setWatchConfigMapsFunction((ignore, handler) -> {
					final CompletableFuture<FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>> future =
						new CompletableFuture<>();
					future.complete(handler);
					configMapCallbackFutures.add(future);
					return new TestingFlinkKubeClient.MockKubernetesWatch();
				})
				.setDeleteConfigMapFunction(name -> {
					configMapStore.remove(name);
					return FutureUtils.completedVoidFuture();
				})
				.setDeleteConfigMapByLabelFunction(labels -> {
					deleteConfigMapByLabelsFuture.complete(labels);
					return FutureUtils.completedVoidFuture();
				})
				.setCloseConsumer(closeKubeClientFuture::complete)
				.setLeaderController(leaderController)
				.build();
		}

		private LeaderElectionDriver createLeaderElectionDriver() {
			final KubernetesLeaderElectionConfiguration leaderConfig = new KubernetesLeaderElectionConfiguration(
				LEADER_CONFIGMAP_NAME, LOCK_IDENTITY, configuration);
			final KubernetesLeaderElectionDriverFactory factory = new KubernetesLeaderElectionDriverFactory(
				flinkKubeClient, executorService, leaderConfig);
			return factory.createLeaderElectionDriver(
				electionEventHandler, electionEventHandler::handleError, LEADER_URL);
		}

		private LeaderRetrievalDriver createLeaderRetrievalDriver() {
			final KubernetesLeaderRetrievalDriverFactory factory = new KubernetesLeaderRetrievalDriverFactory(
				flinkKubeClient, LEADER_CONFIGMAP_NAME);
			return factory.createLeaderRetrievalDriver(retrievalEventHandler, retrievalEventHandler::handleError);
		}
	}

	public static DefaultLeaderElectionService createLeaderElectionService(
			Configuration configuration,
			FlinkKubeClient kubeClient,
			String configMapName,
			String lockIdentity,
			ExecutorService executorService) {
		final KubernetesLeaderElectionConfiguration leaderConfig = new KubernetesLeaderElectionConfiguration(
			configMapName, lockIdentity, configuration);
		return new DefaultLeaderElectionService(new KubernetesLeaderElectionDriverFactory(
			kubeClient, executorService, leaderConfig));
	}

	public static DefaultLeaderRetrievalService createLeaderRetrievalService(
			FlinkKubeClient flinkKubeClient, String configMapName) {
		return new DefaultLeaderRetrievalService(
			new KubernetesLeaderRetrievalDriverFactory(flinkKubeClient, configMapName));
	}
}
