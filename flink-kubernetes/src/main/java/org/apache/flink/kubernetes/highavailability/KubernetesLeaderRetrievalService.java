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
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.apache.flink.kubernetes.utils.Constants.LEADER_ADDRESS_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_SESSION_ID_KEY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The counterpart to the {@link org.apache.flink.kubernetes.highavailability.KubernetesLeaderElectionService}.
 * This implementation of the {@link LeaderRetrievalService} retrieves the current leader which has
 * been elected by the {@link org.apache.flink.kubernetes.highavailability.KubernetesLeaderElectionService}.
 * The leader address as well as the current leader session ID is retrieved from Kubernetes ConfigMap.
 */
class KubernetesLeaderRetrievalService implements LeaderRetrievalService {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesLeaderRetrievalService.class);

	private final Object lock = new Object();

	private final FlinkKubeClient kubeClient;

	private final String configMapName;

	private String lastLeaderAddress;

	private UUID lastLeaderSessionID;

	/**
	 * Listener which will be notified about leader changes.
	 */
	private volatile LeaderRetrievalListener leaderListener;

	private KubernetesWatch kubernetesWatch;

	private volatile boolean running;

	KubernetesLeaderRetrievalService(FlinkKubeClient kubeClient, String configMapName) {
		this.kubeClient = checkNotNull(kubeClient, "Kubernetes client should not be null.");
		this.configMapName = checkNotNull(configMapName, "ConfigMap name should not be null.");

		this.leaderListener = null;
		this.lastLeaderAddress = null;
		this.lastLeaderSessionID = null;

		running = false;
	}

	@Override
	public void start(LeaderRetrievalListener listener) {
		checkNotNull(listener, "Listener must not be null.");
		Preconditions.checkState(leaderListener == null, "KubernetesLeaderRetrievalService can " +
			"only be started once.");

		LOG.info("Starting {}.", this);

		synchronized (lock) {
			running = true;
			leaderListener = listener;
			kubernetesWatch = kubeClient.watchConfigMapsAndDoCallback(
				configMapName, new ConfigMapCallbackHandlerImpl());
		}
	}

	@Override
	public void stop() {
		LOG.info("Stopping {}.", this);

		synchronized (lock) {
			if (!running) {
				return;
			}
			running = false;
		}

		if (kubernetesWatch != null) {
			kubernetesWatch.close();
		}
	}

	@Override
	public String toString() {
		return "KubernetesLeaderRetrievalService{configMapName='" + configMapName + "'}";
	}

	private class ConfigMapCallbackHandlerImpl implements FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> {

		@Override
		public void onAdded(List<KubernetesConfigMap> configMaps) {
			handleEvent(configMaps);
		}

		@Override
		public void onModified(List<KubernetesConfigMap> configMaps) {
			handleEvent(configMaps);
		}

		@Override
		public void onDeleted(List<KubernetesConfigMap> configMaps) {
			LOG.debug("Leader information was lost: The listener will be notified accordingly.");
			leaderListener.notifyLeaderAddress(null, null);
		}

		@Override
		public void onError(List<KubernetesConfigMap> configMaps) {
			leaderListener.handleError(new Exception("Error while watching the ConfigMap " + configMapName));
		}

		@Override
		public void handleFatalError(Throwable throwable) {
			leaderListener.handleError(
				new Exception("Fatal error while watching the ConfigMap " + configMapName, throwable));
		}

		private void handleEvent(List<KubernetesConfigMap> configMaps) {
			configMaps.forEach(e -> {
				final String leaderAddress = e.getData().get(LEADER_ADDRESS_KEY);
				final String sessionID = e.getData().get(LEADER_SESSION_ID_KEY);
				if (leaderAddress != null && sessionID != null) {
					final UUID leaderSessionID = UUID.fromString(sessionID);
					if (!(Objects.equals(leaderAddress, lastLeaderAddress) &&
						Objects.equals(leaderSessionID, lastLeaderSessionID))) {
						LOG.debug(
							"New leader information: Leader={}, session ID={}.",
							leaderAddress,
							leaderSessionID);

						lastLeaderAddress = leaderAddress;
						lastLeaderSessionID = leaderSessionID;
						leaderListener.notifyLeaderAddress(leaderAddress, leaderSessionID);
					}
				} else {
					LOG.debug("Leader information was lost: The listener will be notified accordingly.");
					leaderListener.notifyLeaderAddress(
						leaderAddress, sessionID == null ? null : UUID.fromString(sessionID));
				}
			});
		}
	}
}
