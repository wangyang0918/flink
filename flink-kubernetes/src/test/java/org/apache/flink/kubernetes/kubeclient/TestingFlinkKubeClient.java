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

import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.FunctionWithException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.flink.kubernetes.utils.Constants.LEADER_ANNOTATION_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LOCK_IDENTITY;

/**
 * Testing implementation of {@link FlinkKubeClient}.
 */
public class TestingFlinkKubeClient implements FlinkKubeClient {

	private final Function<KubernetesPod, CompletableFuture<Void>> createTaskManagerPodFunction;
	private final Function<String, CompletableFuture<Void>> stopPodFunction;
	private final Consumer<String> stopAndCleanupClusterConsumer;
	private final Function<Map<String, String>, List<KubernetesPod>> getPodsWithLabelsFunction;
	private final BiFunction<Map<String, String>, WatchCallbackHandler<KubernetesPod>, KubernetesWatch> watchPodsAndDoCallbackFunction;
	private final BiFunction<String, WatchCallbackHandler<KubernetesConfigMap>, KubernetesWatch> watchConfigMapsAndDoCallbackFunction;
	private final Map<String, KubernetesConfigMap> configMapStore;
	private final AtomicBoolean leaderController;
	private final Consumer<Map<String, String>> deleteConfigMapsConsumer;

	private TestingFlinkKubeClient(
			Function<KubernetesPod, CompletableFuture<Void>> createTaskManagerPodFunction,
			Function<String, CompletableFuture<Void>> stopPodFunction,
			Consumer<String> stopAndCleanupClusterConsumer,
			Function<Map<String, String>, List<KubernetesPod>> getPodsWithLabelsFunction,
			BiFunction<Map<String, String>, WatchCallbackHandler<KubernetesPod>, KubernetesWatch> watchPodsAndDoCallbackFunction,
			BiFunction<String, WatchCallbackHandler<KubernetesConfigMap>, KubernetesWatch> watchConfigMapsAndDoCallbackFunction,
			Map<String, KubernetesConfigMap> configMapStore,
			AtomicBoolean leaderController,
			Consumer<Map<String, String>> deleteConfigMapsConsumer) {

		this.createTaskManagerPodFunction = createTaskManagerPodFunction;
		this.stopPodFunction = stopPodFunction;
		this.stopAndCleanupClusterConsumer = stopAndCleanupClusterConsumer;
		this.getPodsWithLabelsFunction = getPodsWithLabelsFunction;
		this.watchPodsAndDoCallbackFunction = watchPodsAndDoCallbackFunction;
		this.watchConfigMapsAndDoCallbackFunction = watchConfigMapsAndDoCallbackFunction;
		this.configMapStore = configMapStore;
		this.leaderController = leaderController;
		this.deleteConfigMapsConsumer = deleteConfigMapsConsumer;
	}

	@Override
	public void createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Void> createTaskManagerPod(KubernetesPod kubernetesPod) {
		return createTaskManagerPodFunction.apply(kubernetesPod);
	}

	@Override
	public CompletableFuture<Void> stopPod(String podName) {
		return stopPodFunction.apply(podName);
	}

	@Override
	public void stopAndCleanupCluster(String clusterId) {
		stopAndCleanupClusterConsumer.accept(clusterId);
	}

	@Override
	public Optional<KubernetesService> getRestService(String clusterId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Optional<Endpoint> getRestEndpoint(String clusterId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<KubernetesPod> getPodsWithLabels(Map<String, String> labels) {
		return getPodsWithLabelsFunction.apply(labels);
	}

	@Override
	public void handleException(Exception e) {
		throw new UnsupportedOperationException();
	}

	@Override
	public KubernetesWatch watchPodsAndDoCallback(Map<String, String> labels, WatchCallbackHandler<KubernetesPod> podCallbackHandler) {
		return watchPodsAndDoCallbackFunction.apply(labels, podCallbackHandler);
	}

	@Override
	public KubernetesLeaderElector createLeaderElector(KubernetesLeaderElectionConfiguration leaderElectionConfiguration, KubernetesLeaderElector.LeaderCallbackHandler leaderCallbackHandler) {
		return new MockKubernetesLeaderElector(leaderElectionConfiguration, leaderCallbackHandler, leaderController);
	}

	@Override
	public CompletableFuture<Void> createConfigMap(KubernetesConfigMap configMap) {
		configMapStore.putIfAbsent(configMap.getName(), configMap);
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public Optional<KubernetesConfigMap> getConfigMap(String name) {
		final KubernetesConfigMap configMap = configMapStore.get(name);
		if (configMap == null) {
			return Optional.empty();
		}
		return Optional.of(new MockKubernetesConfigMap(configMap.getName(), new HashMap<>(configMap.getData())));
	}

	@Override
	public CompletableFuture<Boolean> checkAndUpdateConfigMap(
			String configMapName,
			Predicate<KubernetesConfigMap> checker,
			FunctionWithException<KubernetesConfigMap, KubernetesConfigMap, ?> function) {
		return getConfigMap(configMapName).map(FunctionUtils.uncheckedFunction(
			configMap -> {
				final boolean shouldUpdate = checker.test(configMap);
				if (shouldUpdate) {
					configMapStore.put(configMap.getName(), function.apply(configMap));
				}
				return CompletableFuture.completedFuture(shouldUpdate);
			}))
			.orElseThrow(() -> new FlinkRuntimeException("ConfigMap " + configMapName + " not exists."));
	}

	@Override
	public KubernetesWatch watchConfigMapsAndDoCallback(String name, WatchCallbackHandler<KubernetesConfigMap> callbackHandler) {
		return watchConfigMapsAndDoCallbackFunction.apply(name, callbackHandler);
	}

	@Override
	public void deleteConfigMapsByLabels(Map<String, String> labels) {
		deleteConfigMapsConsumer.accept(labels);
	}

	@Override
	public void deleteConfigMap(String configMapName) {
		configMapStore.remove(configMapName);
	}

	@Override
	public void close() throws Exception {
		// noop
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder class for {@link TestingFlinkKubeClient}.
	 */
	public static class Builder {
		private Function<KubernetesPod, CompletableFuture<Void>> createTaskManagerPodFunction =
				(ignore) -> FutureUtils.completedVoidFuture();
		private Function<String, CompletableFuture<Void>> stopPodFunction =
				(ignore) -> FutureUtils.completedVoidFuture();
		private Consumer<String> stopAndCleanupClusterConsumer =
				(ignore) -> {};
		private Function<Map<String, String>, List<KubernetesPod>> getPodsWithLabelsFunction =
				(ignore) -> Collections.emptyList();
		private BiFunction<Map<String, String>, WatchCallbackHandler<KubernetesPod>, KubernetesWatch> watchPodsAndDoCallbackFunction =
				(ignore1, ignore2) -> new MockKubernetesWatch();
		private BiFunction<String, WatchCallbackHandler<KubernetesConfigMap>, KubernetesWatch> watchConfigMapsAndDoCallbackFunction =
			(ignore1, ignore2) -> new MockKubernetesWatch();
		private Map<String, KubernetesConfigMap> configMapStore = new HashMap<>();
		private AtomicBoolean leaderController = new AtomicBoolean(false);
		private Consumer<Map<String, String>> deleteConfigMapsConsumer;

		private Builder() {}

		public Builder setCreateTaskManagerPodFunction(Function<KubernetesPod, CompletableFuture<Void>> createTaskManagerPodFunction) {
			this.createTaskManagerPodFunction = Preconditions.checkNotNull(createTaskManagerPodFunction);
			return this;
		}

		public Builder setStopPodFunction(Function<String, CompletableFuture<Void>> stopPodFunction) {
			this.stopPodFunction = Preconditions.checkNotNull(stopPodFunction);
			return this;
		}

		public Builder setStopAndCleanupClusterConsumer(Consumer<String> stopAndCleanupClusterConsumer) {
			this.stopAndCleanupClusterConsumer = Preconditions.checkNotNull(stopAndCleanupClusterConsumer);
			return this;
		}

		public Builder setGetPodsWithLabelsFunction(Function<Map<String, String>, List<KubernetesPod>> getPodsWithLabelsFunction) {
			this.getPodsWithLabelsFunction = Preconditions.checkNotNull(getPodsWithLabelsFunction);
			return this;
		}

		public Builder setWatchPodsAndDoCallbackFunction(BiFunction<Map<String, String>, WatchCallbackHandler<KubernetesPod>, KubernetesWatch> watchPodsAndDoCallbackFunction) {
			this.watchPodsAndDoCallbackFunction = Preconditions.checkNotNull(watchPodsAndDoCallbackFunction);
			return this;
		}

		public Builder setWatchConfigMapsAndDoCallbackFunction(BiFunction<String, WatchCallbackHandler<KubernetesConfigMap>, KubernetesWatch> watchConfigMapsAndDoCallbackFunction) {
			this.watchConfigMapsAndDoCallbackFunction = Preconditions.checkNotNull(watchConfigMapsAndDoCallbackFunction);
			return this;
		}

		public Builder setConfigMapStore(Map<String, KubernetesConfigMap> configMapStore) {
			this.configMapStore = configMapStore;
			return this;
		}

		public Builder setLeaderController(AtomicBoolean leaderController) {
			this.leaderController = leaderController;
			return this;
		}

		public Builder setDeleteConfigMapsConsumer(Consumer<Map<String, String>> consumer) {
			this.deleteConfigMapsConsumer = consumer;
			return this;
		}

		public TestingFlinkKubeClient build() {
			return new TestingFlinkKubeClient(
					createTaskManagerPodFunction,
					stopPodFunction,
					stopAndCleanupClusterConsumer,
					getPodsWithLabelsFunction,
					watchPodsAndDoCallbackFunction,
					watchConfigMapsAndDoCallbackFunction,
					configMapStore,
					leaderController,
					deleteConfigMapsConsumer);
		}
	}

	/**
	 * Testing implementation of {@link KubernetesWatch}.
	 */
	public static class MockKubernetesWatch extends KubernetesWatch {
		public MockKubernetesWatch() {
			super(null);
		}

		@Override
		public void close() {
			// noop
		}
	}

	/**
	 * Testing implementation of {@link KubernetesConfigMap}.
	 */
	private class MockKubernetesConfigMap extends KubernetesConfigMap {
		private final String name;
		private final Map<String, String> data;
		private final Map<String, String> labels;
		MockKubernetesConfigMap(String name, Map<String, String> data) {
			super(null);
			this.name = name;
			this.data = data;
			this.labels = new HashMap<>();
		}

		@Override
		public String getName() {
			return this.name;
		}

		@Override
		public Map<String, String> getData() {
			return this.data;
		}

		@Override
		public Map<String, String> getAnnotations() {
			final Map<String, String> annotations = new HashMap<>();
			annotations.put(LEADER_ANNOTATION_KEY, LOCK_IDENTITY);
			return annotations;
		}

		@Override
		public Map<String, String> getLabels() {
			return this.labels;
		}
	}

	/**
	 * Testing implementation of {@link KubernetesLeaderElector}.
	 */
	private class MockKubernetesLeaderElector extends KubernetesLeaderElector {
		private final AtomicBoolean leaderController;
		private final KubernetesLeaderElectionConfiguration leaderConfig;
		private final LeaderCallbackHandler leaderCallbackHandler;
		private static final String NAMESPACE = "test";
		MockKubernetesLeaderElector(
				KubernetesLeaderElectionConfiguration leaderConfig,
				LeaderCallbackHandler leaderCallbackHandler,
				AtomicBoolean leaderController) {
			super(null, NAMESPACE, leaderConfig, leaderCallbackHandler);
			this.leaderConfig = leaderConfig;
			this.leaderCallbackHandler = leaderCallbackHandler;
			this.leaderController = leaderController;
		}

		@Override
		public void run() {
			// Try acquire, please set the leaderController manually if you want to start/stop leading
			while (!leaderController.get()) {
				try {
					Thread.sleep(leaderConfig.getRetryPeriod().toMillis());
				} catch (InterruptedException e) {
					return;
				}
			}
			configMapStore.put(
				leaderConfig.getConfigMapName(),
				new MockKubernetesConfigMap(leaderConfig.getConfigMapName(), new HashMap<>()));
			leaderCallbackHandler.isLeader();
			// Try to keep the leader position
			while (leaderController.get()) {
				try {
					Thread.sleep(leaderConfig.getRetryPeriod().toMillis());
				} catch (InterruptedException e) {
					return;
				}
			}
			leaderCallbackHandler.notLeader();
		}
	}
}
