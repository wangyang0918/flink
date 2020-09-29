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

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.leaderelection.AbstractLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderContender;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_ADDRESS_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_SESSION_ID_KEY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Leader election service for multiple JobManagers. The active JobManager is elected using Kubernetes.
 * The current leader's address as well as its leader session ID is published via Kubernetes ConfigMap as well.
 * Note that the contending lock and leader storage are using the same ConfigMap. And every election service(e.g.
 * ResourceManager, Dispatcher, RestEndpoint, JobManager for each job) will have a separate ConfigMap.
 */
public class KubernetesLeaderElectionService extends AbstractLeaderElectionService {

	private final FlinkKubeClient kubeClient;

	private final Executor executor;

	private final String clusterId;
	private final String configMapName;

	private final KubernetesLeaderElector leaderElector;

	private KubernetesWatch kubernetesWatch;

	KubernetesLeaderElectionService(
			FlinkKubeClient kubeClient,
			Executor executor,
			KubernetesLeaderElectionConfiguration leaderConfig) {

		this.kubeClient = checkNotNull(kubeClient, "Kubernetes client should not be null.");
		this.executor = checkNotNull(executor, "Executor should not be null.");
		this.clusterId = leaderConfig.getClusterId();
		this.configMapName = leaderConfig.getConfigMapName();
		this.leaderElector = kubeClient.createLeaderElector(leaderConfig, new LeaderCallbackHandlerImpl());
		this.leaderContender = null;
	}

	@Override
	public void internalStart(LeaderContender contender) {
		CompletableFuture.runAsync(leaderElector::run, executor);
		kubernetesWatch = kubeClient.watchConfigMapsAndDoCallback(configMapName, new ConfigMapCallbackHandlerImpl());
	}

	@Override
	public void internalStop() {
		if (kubernetesWatch != null) {
			kubernetesWatch.close();
		}
	}

	@Override
	public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
		return leaderSessionId.equals(issuedLeaderSessionID);
	}

	@Override
	public String toString() {
		return "KubernetesLeaderElectionService{configMapName='" + configMapName + "'}";
	}

	protected void writeLeaderInformation() {
		Optional<KubernetesConfigMap> optional = kubeClient.getConfigMap(configMapName);
		if (optional.isPresent()) {
			final KubernetesConfigMap configMap = optional.get();
			configMap.setData(getCurrentLeaderData());
			updateConfigMapWithLabels(configMap);
		} else {
			logger.debug("Leader ConfigMap {} does not exist!", configMapName);
		}
	}

	@Override
	protected boolean checkLeaderLatch() {
		return leaderElector.hasLeadership();
	}

	private Map<String, String> getCurrentLeaderData() {
		final Map<String, String> data = new HashMap<>();
		if (confirmedLeaderAddress != null && confirmedLeaderSessionID != null) {
			data.put(LEADER_ADDRESS_KEY, confirmedLeaderAddress);
			data.put(LEADER_SESSION_ID_KEY, confirmedLeaderSessionID.toString());
		}
		return data;
	}

	private void updateConfigMapWithLabels(KubernetesConfigMap configMap) {
		configMap.setLabels(
			KubernetesUtils.getConfigMapLabels(clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
		try {
			kubeClient.updateConfigMap(configMap).get();
		} catch (Exception e) {
			leaderContender.handleError(
				new Exception("Could not write leader address and leader session ID to ConfigMap " + configMapName, e));
		}
	}

	private class LeaderCallbackHandlerImpl extends KubernetesLeaderElector.LeaderCallbackHandler {

		@Override
		public void isLeader() {
			onGrantLeadership();
		}

		@Override
		public void notLeader() {
			// Clear the leader information in ConfigMap
			kubeClient.getConfigMap(configMapName).ifPresent(configMap -> {
				configMap.setData(new HashMap<>());
				updateConfigMapWithLabels(configMap);
			});
			onRevokeLeadership();
			// Continue to contend the leader
			CompletableFuture.runAsync(leaderElector::run, executor);
		}
	}

	private class ConfigMapCallbackHandlerImpl implements FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> {

		@Override
		public void onAdded(List<KubernetesConfigMap> configMaps) {
			// noop
		}

		@Override
		public void onModified(List<KubernetesConfigMap> configMaps) {
			handleEvent(configMaps, configMap -> {
				if (configMap.getData() != null && isLeaderChanged(configMap)) {
					// the data field does not correspond to the expected leader information
					if (logger.isDebugEnabled()) {
						logger.debug("Correcting leader information in {} by {}.",
							configMapName, leaderContender.getDescription());
					}
					configMap.setData(getCurrentLeaderData());
					updateConfigMapWithLabels(configMap);
				}
			});
		}

		@Override
		public void onDeleted(List<KubernetesConfigMap> configMaps) {
			handleEvent(configMaps, configMap -> {
				// the ConfigMap is deleted externally, create a new one
				if (logger.isDebugEnabled()) {
					logger.debug("Writing leader information into a new ConfigMap {} by {}.",
						configMapName, leaderContender.getDescription());
				}
				configMap.setData(getCurrentLeaderData());
				updateConfigMapWithLabels(configMap);
			});
		}

		@Override
		public void onError(List<KubernetesConfigMap> configMaps) {
			leaderContender.handleError(new Exception("Error while watching the configMap " + configMapName));
		}

		@Override
		public void handleFatalError(Throwable throwable) {
			leaderContender.handleError(new Exception("Fatal error while watching the configMap " + configMapName));
		}

		private void handleEvent(List<KubernetesConfigMap> configMaps, Consumer<KubernetesConfigMap> consumer) {
			try {
				if (leaderElector.hasLeadership()) {
					synchronized (lock) {
						if (running) {
							configMaps.forEach(consumer);
						}
					}
				}
			} catch (Exception e) {
				leaderContender.handleError(new Exception("Could not handle ConfigMap changed event.", e));
				throw e;
			}
		}

		private boolean isLeaderChanged(KubernetesConfigMap configMap) {
			checkNotNull(configMap);
			checkNotNull(configMap.getData());
			final String leaderAddress = configMap.getData().get(LEADER_ADDRESS_KEY);
			final String leaderSessionID = configMap.getData().get(LEADER_SESSION_ID_KEY);
			return !(Objects.equals(leaderAddress, confirmedLeaderAddress) &&
				Objects.equals(leaderSessionID, confirmedLeaderSessionID.toString()));
		}
	}
}
