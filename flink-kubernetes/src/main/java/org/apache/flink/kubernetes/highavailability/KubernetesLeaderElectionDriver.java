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

import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesException;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.leaderelection.LeaderElectionDriver;
import org.apache.flink.runtime.leaderelection.LeaderElectionEventHandler;
import org.apache.flink.runtime.leaderelection.LeaderElectionException;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_ADDRESS_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_SESSION_ID_KEY;
import static org.apache.flink.kubernetes.utils.KubernetesUtils.checkConfigMaps;
import static org.apache.flink.kubernetes.utils.KubernetesUtils.getLeaderInformationFromConfigMap;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link LeaderElectionDriver} implementation for Kubernetes. The active leader is elected using Kubernetes.
 * The current leader's address as well as its leader session ID is published via Kubernetes ConfigMap.
 * Note that the contending lock and leader storage are using the same ConfigMap. And every component(e.g.
 * ResourceManager, Dispatcher, RestEndpoint, JobManager for each job) will have a separate ConfigMap.
 */
public class KubernetesLeaderElectionDriver implements LeaderElectionDriver {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesLeaderElectionDriver.class);

	private final Object lock = new Object();

	private final FlinkKubeClient kubeClient;

	private final Executor ioExecutor;

	private final String configMapName;

	private final String lockIdentity;

	private final KubernetesLeaderElector leaderElector;

	// Labels will be used to clean up the ha related ConfigMaps.
	private final Map<String, String> configMapLabels;

	private final LeaderElectionEventHandler leaderElectionEventHandler;

	private final KubernetesWatch kubernetesWatch;

	private final FatalErrorHandler fatalErrorHandler;

	@GuardedBy("lock")
	private volatile CompletableFuture<Void> leaderElectorRunFuture;

	@GuardedBy("lock")
	private volatile boolean running;

	public KubernetesLeaderElectionDriver(
			FlinkKubeClient kubeClient,
			Executor ioExecutor,
			KubernetesLeaderElectionConfiguration leaderConfig,
			LeaderElectionEventHandler leaderElectionEventHandler,
			FatalErrorHandler fatalErrorHandler) {

		this.kubeClient = checkNotNull(kubeClient, "Kubernetes client");
		this.ioExecutor = checkNotNull(ioExecutor, "IO Executor");

		this.leaderElectionEventHandler = checkNotNull(leaderElectionEventHandler, "LeaderElectionEventHandler");
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);

		this.configMapName = leaderConfig.getConfigMapName();
		this.lockIdentity = leaderConfig.getLockIdentity();
		this.leaderElector = kubeClient.createLeaderElector(leaderConfig, new LeaderCallbackHandlerImpl());
		this.configMapLabels = KubernetesUtils.getConfigMapLabels(
			leaderConfig.getClusterId(), LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY);

		leaderElectorRunFuture = CompletableFuture.runAsync(leaderElector::run, ioExecutor);
		FutureUtils.assertNoException(leaderElectorRunFuture);
		kubernetesWatch = kubeClient.watchConfigMaps(configMapName, new ConfigMapCallbackHandlerImpl());
		running = true;
	}

	@Override
	public void close() {
		synchronized (lock) {
			if (!running) {
				return;
			}
			running = false;

			LOG.info("Closing {}.", this);
			leaderElectorRunFuture.complete(null);
			kubernetesWatch.close();
		}
	}

	@Override
	public void writeLeaderInformation(LeaderInformation leaderInformation) {
		final UUID confirmedLeaderSessionID = leaderInformation.getLeaderSessionID();
		final String confirmedLeaderAddress = leaderInformation.getLeaderAddress();
		try {
			kubeClient.checkAndUpdateConfigMap(
				configMapName,
				configMap -> {
					if (KubernetesLeaderElector.hasLeadership(configMap, lockIdentity)) {
						// Get the updated ConfigMap with new leader information
						if (confirmedLeaderAddress != null && confirmedLeaderSessionID != null) {
							configMap.getData().put(LEADER_ADDRESS_KEY, confirmedLeaderAddress);
							configMap.getData().put(LEADER_SESSION_ID_KEY, confirmedLeaderSessionID.toString());
						}
						configMap.getLabels().putAll(configMapLabels);
						return Optional.of(configMap);
					}
					return Optional.empty();
				}).get();
		} catch (Exception e) {
			fatalErrorHandler.onFatalError(
				new KubernetesException("Could not update ConfigMap " + configMapName, e));
		}
	}

	@Override
	public boolean hasLeadership() {
		final Optional<KubernetesConfigMap> configMapOpt = kubeClient.getConfigMap(configMapName);
		if (configMapOpt.isPresent()) {
			return KubernetesLeaderElector.hasLeadership(configMapOpt.get(), lockIdentity);
		} else {
			fatalErrorHandler.onFatalError(
				new KubernetesException("ConfigMap " + configMapName + "does not exist.", null));
			return false;
		}
	}

	private class LeaderCallbackHandlerImpl extends KubernetesLeaderElector.LeaderCallbackHandler {

		@Override
		public void isLeader() {
			synchronized (lock) {
				if (running) {
					leaderElectionEventHandler.onGrantLeadership();
				}
			}
		}

		@Override
		public void notLeader() {
			synchronized (lock) {
				if (running) {
					// Clear the leader information in ConfigMap
					try {
						kubeClient.checkAndUpdateConfigMap(
							configMapName,
							configMap -> {
								if (KubernetesLeaderElector.hasLeadership(configMap, lockIdentity)) {
									configMap.getData().remove(LEADER_ADDRESS_KEY);
									configMap.getData().remove(LEADER_SESSION_ID_KEY);
									return Optional.of(configMap);
								}
								return Optional.empty();
							}
						).get();
					} catch (Exception e) {
						fatalErrorHandler.onFatalError(
							new LeaderElectionException(
								"Could not remove leader information from ConfigMap " + configMapName, e));
					}
					leaderElectionEventHandler.onRevokeLeadership();
					// Continue to contend the leader
					if (!leaderElectorRunFuture.isDone()) {
						leaderElectorRunFuture.complete(null);
					}
					leaderElectorRunFuture = CompletableFuture.runAsync(leaderElector::run, ioExecutor);
					FutureUtils.assertNoException(leaderElectorRunFuture);
				}
			}
		}
	}

	private class ConfigMapCallbackHandlerImpl implements FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> {
		// This is used to get the difference between current and previous data. Since the annotation will be updated
		// periodically, we need to filter out these noises.
		final Map<String, String> previousData = new HashMap<>();

		@Override
		public void onAdded(List<KubernetesConfigMap> configMaps) {
			// noop
		}

		@Override
		public void onModified(List<KubernetesConfigMap> configMaps) {
			// We should only receive events for the watched ConfigMap
			final KubernetesConfigMap configMap = checkConfigMaps(configMaps, configMapName);

			if (!configMap.getData().equals(previousData)) {
				if (KubernetesLeaderElector.hasLeadership(configMap, lockIdentity)) {
					synchronized (lock) {
						if (running) {
							leaderElectionEventHandler.onLeaderInformationChange(
								getLeaderInformationFromConfigMap(configMap));
						}
					}
				}
				previousData.clear();
				previousData.putAll(configMap.getData());
			}
		}

		@Override
		public void onDeleted(List<KubernetesConfigMap> configMaps) {
			final KubernetesConfigMap configMap = checkConfigMaps(configMaps, configMapName);
			// The ConfigMap is deleted externally.
			if (KubernetesLeaderElector.hasLeadership(configMap, lockIdentity)) {
				handleFatalError("ConfigMap " + configMapName + " is deleted externally", null);
			}
		}

		@Override
		public void onError(List<KubernetesConfigMap> configMaps) {
			final KubernetesConfigMap configMap = checkConfigMaps(configMaps, configMapName);
			if (KubernetesLeaderElector.hasLeadership(configMap, lockIdentity)) {
				handleFatalError("Error while watching the configMap " + configMapName, null);
			}
		}

		@Override
		public void handleFatalError(Throwable throwable) {
			handleFatalError("Fatal error while watching the configMap " + configMapName, throwable);
		}

		@GuardedBy("lock")
		private void handleFatalError(String errorMsg, @Nullable Throwable throwable) {
			synchronized (lock) {
				if (running) {
					fatalErrorHandler.onFatalError(new LeaderElectionException(errorMsg, throwable));
				}
			}
		}
	}

	@Override
	public String toString() {
		return "KubernetesLeaderElectionDriver{configMapName='" + configMapName + "'}";
	}
}
