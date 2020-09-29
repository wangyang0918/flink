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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesHighAvailabilityOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.runtime.zookeeper.filesystem.FileSystemStateStorageHelper;

import java.util.concurrent.Executor;

import static org.apache.flink.kubernetes.utils.Constants.NAME_SEPARATOR;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CheckpointCoordinator} components in {@link org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory}.
 */
public class KubernetesCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

	private static final String COMPLETED_CHECKPOINT_FILE_SUFFIX = "completedCheckpoint";

	private final FlinkKubeClient kubeClient;

	private final Executor executor;

	private final String clusterId;

	private final int maxRetryAttempts;

	private final Configuration configuration;

	private final String checkpointsSuffix;
	private final String checkpointCounterSuffix;

	public KubernetesCheckpointRecoveryFactory(
			FlinkKubeClient kubeClient,
			Configuration configuration,
			Executor executor) {

		this.kubeClient = checkNotNull(kubeClient, "Kubernetes client");
		this.configuration = checkNotNull(configuration, "configuration");
		this.executor = checkNotNull(executor, "Executor");
		this.clusterId = checkNotNull(
			configuration.getString(KubernetesConfigOptions.CLUSTER_ID), "Cluster ID");
		this.maxRetryAttempts = configuration.getInteger(
			KubernetesHighAvailabilityOptions.KUBERNETES_MAX_RETRY_ATTEMPTS);

		this.checkpointsSuffix = configuration.getString(
			KubernetesHighAvailabilityOptions.HA_KUBERNETES_CHECKPOINTS_SUFFIX);
		this.checkpointCounterSuffix = configuration.getString(
			KubernetesHighAvailabilityOptions.HA_KUBERNETES_CHECKPOINT_COUNTER_SUFFIX);
	}

	@Override
	public CompletedCheckpointStore createCheckpointStore(
			JobID jobID,
			int maxNumberOfCheckpointsToRetain,
			ClassLoader userClassLoader) throws Exception {

		final RetrievableStateStorageHelper<CompletedCheckpoint> stateStorage =
			new FileSystemStateStorageHelper<>(HighAvailabilityServicesUtils
				.getClusterHighAvailableStoragePath(configuration), COMPLETED_CHECKPOINT_FILE_SUFFIX);
		final String configMapName = getConfigMapNameFor(jobID, checkpointsSuffix);
		return new KubernetesCompletedCheckpointStore(
			kubeClient,
			configMapName,
			maxNumberOfCheckpointsToRetain,
			new KubernetesStateHandleStore<>(kubeClient, executor, configMapName, stateStorage, maxRetryAttempts),
			executor);
	}

	@Override
	public CheckpointIDCounter createCheckpointIDCounter(JobID jobID) {
		return new KubernetesCheckpointIDCounter(
			kubeClient,
			executor,
			getConfigMapNameFor(jobID, checkpointCounterSuffix),
			maxRetryAttempts);
	}

	private String getConfigMapNameFor(JobID jobID, String suffix) {
		return clusterId + NAME_SEPARATOR + jobID.toString() + NAME_SEPARATOR + suffix;
	}
}
