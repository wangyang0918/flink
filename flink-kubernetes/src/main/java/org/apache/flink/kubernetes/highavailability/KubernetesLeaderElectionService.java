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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_ADDRESS_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_SESSION_ID_KEY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Leader election service for multiple JobManagers. The active JobManager is elected using Kubernetes.
 * The current leader's address as well as its leader session ID is published via Kubernetes ConfigMap.
 * Note that the contending lock and leader storage are using the same ConfigMap. And every component(e.g.
 * ResourceManager, Dispatcher, RestEndpoint, JobManager for each job) will have a separate ConfigMap.
 */
public class KubernetesLeaderElectionService extends AbstractLeaderElectionService {

	private final FlinkKubeClient kubeClient;

	private final Executor executor;

	private final String configMapName;

	private final KubernetesLeaderElector leaderElector;

	private KubernetesWatch kubernetesWatch;

	// Labels will be used to clean up the ha related ConfigMaps.
	private Map<String, String> configMapLabels;

	KubernetesLeaderElectionService(
			FlinkKubeClient kubeClient,
			Executor executor,
			KubernetesLeaderElectionConfiguration leaderConfig) {

		this.kubeClient = checkNotNull(kubeClient, "Kubernetes client should not be null.");
		this.executor = checkNotNull(executor, "Executor should not be null.");
		this.configMapName = leaderConfig.getConfigMapName();
		this.leaderElector = kubeClient.createLeaderElector(leaderConfig, new LeaderCallbackHandlerImpl());
		this.leaderContender = null;
		this.configMapLabels = KubernetesUtils.getConfigMapLabels(
			leaderConfig.getClusterId(), LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY);
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
	public String toString() {
		return "KubernetesLeaderElectionService{configMapName='" + configMapName + "'}";
	}

	@Override
	protected void writeLeaderInformation() {
		updateConfigMap(configMapName);
	}

	@Override
	protected boolean checkLeaderLatch() {
		return leaderElector.hasLeadership();
	}

	private void updateConfigMap(String configMapName) {
		try {
			kubeClient.checkAndUpdateConfigMap(
				configMapName,
				KubernetesUtils.getLeaderChecker(),
				configMap -> {
					// Get the updated ConfigMap with new leader information
					if (confirmedLeaderAddress != null && confirmedLeaderSessionID != null) {
						configMap.getData().put(LEADER_ADDRESS_KEY, confirmedLeaderAddress);
						configMap.getData().put(LEADER_SESSION_ID_KEY, confirmedLeaderSessionID.toString());
					}
					configMap.getLabels().putAll(configMapLabels);
					return configMap;
				}).get();
		} catch (Exception e) {
			leaderContender.handleError(new Exception("Could not update ConfigMap " + configMapName, e));
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
			try {
				kubeClient.checkAndUpdateConfigMap(
					configMapName,
					KubernetesUtils.getLeaderChecker(),
					configMap -> {
						configMap.getData().remove(LEADER_ADDRESS_KEY);
						configMap.getData().remove(LEADER_SESSION_ID_KEY);
						return configMap;
					}
				).get();
			} catch (Exception e) {
				leaderContender.handleError(
					new Exception("Could not remove leader information from ConfigMap " + configMapName, e));
			}
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
				if (isLeaderChanged(configMap)) {
					// the data field does not correspond to the expected leader information
					if (logger.isDebugEnabled()) {
						logger.debug("Correcting leader information in {} by {}.",
							configMapName, leaderContender.getDescription());
					}
					updateConfigMap(configMap.getName());
				}
			});
		}

		@Override
		public void onDeleted(List<KubernetesConfigMap> configMaps) {
			handleEvent(configMaps, configMap -> {
				// the ConfigMap is deleted externally, create a new one
				if (logger.isDebugEnabled()) {
					logger.debug("Writing leader information into a new ConfigMap {} by {}.",
						configMap.getName(), leaderContender.getDescription());
				}
				kubeClient.createConfigMap(configMap);
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
				Objects.equals(
					leaderSessionID, confirmedLeaderSessionID == null ? null : confirmedLeaderSessionID.toString()));
		}
	}
}
