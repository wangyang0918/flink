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
import org.apache.flink.kubernetes.kubeclient.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.runtime.zookeeper.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.NAME_SEPARATOR;

/**
 * High availability service for Kubernetes.
 */
public class KubernetesHaServices implements HighAvailabilityServices {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesHaServices.class);

	private static final String RESOURCE_MANAGER_LEADER_NAME = "resourcemanager";

	private static final String DISPATCHER_LEADER_NAME = "dispatcher";

	private static final String JOB_MANAGER_LEADER_NAME = "jobmanager";

	private static final String REST_SERVER_LEADER_NAME = "restserver";

	private static final String SUBMITTED_JOBGRAPH_FILE_PREFIX = "submittedJobGraph";

	private final String leaderSuffix;

	private final String runningJobRegistryName;

	private final String jobGraphStoreName;

	private final String clusterId;

	/** Kubernetes client. */
	private final FlinkKubeClient kubeClient;

	/** The executor to run ZooKeeper callbacks on. */
	private final Executor executor;

	/** The runtime configuration. */
	private final Configuration configuration;

	/** Store for arbitrary blobs. */
	private final BlobStoreService blobStoreService;

	/** The Kubernetes based running jobs registry. */
	private final RunningJobsRegistry runningJobsRegistry;

	KubernetesHaServices(
			FlinkKubeClient kubeClient,
			Executor executor,
			Configuration configuration,
			BlobStoreService blobStoreService) {

		this.kubeClient = kubeClient;
		this.executor = executor;
		this.configuration = configuration;
		this.clusterId = configuration.get(KubernetesConfigOptions.CLUSTER_ID);
		this.blobStoreService = blobStoreService;

		this.leaderSuffix = configuration.getString(KubernetesHighAvailabilityOptions.HA_KUBERNETES_LEADER_SUFFIX);
		this.runningJobRegistryName = configuration.getString(
			KubernetesHighAvailabilityOptions.HA_KUBERNETES_RUNNING_JOB_REGISTRY_SUFFIX);
		this.jobGraphStoreName = configuration.getString(
			KubernetesHighAvailabilityOptions.HA_KUBERNETES_JOBGRAPHS_SUFFIX);

		this.runningJobsRegistry = new KubernetesRunningJobsRegistry(
			kubeClient, clusterId + NAME_SEPARATOR + runningJobRegistryName);
	}

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() {
		return createLeaderRetrievalService(RESOURCE_MANAGER_LEADER_NAME);
	}

	@Override
	public LeaderRetrievalService getDispatcherLeaderRetriever() {
		return createLeaderRetrievalService(DISPATCHER_LEADER_NAME);
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
		return createLeaderRetrievalService(getLeaderNameForJobManager(jobID));
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress) {
		return getJobManagerLeaderRetriever(jobID);
	}

	@Override
	public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
		return createLeaderRetrievalService(REST_SERVER_LEADER_NAME);
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		return createLeaderElectionService(RESOURCE_MANAGER_LEADER_NAME);
	}

	@Override
	public LeaderElectionService getDispatcherLeaderElectionService() {
		return createLeaderElectionService(DISPATCHER_LEADER_NAME);
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
		return createLeaderElectionService(getLeaderNameForJobManager(jobID));
	}

	@Override
	public LeaderElectionService getClusterRestEndpointLeaderElectionService() {
		return createLeaderElectionService(REST_SERVER_LEADER_NAME);
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		return new KubernetesCheckpointRecoveryFactory(kubeClient, configuration, executor, clusterId);
	}

	@Override
	public JobGraphStore getJobGraphStore() throws Exception {
		final RetrievableStateStorageHelper<JobGraph> stateStorage =
			new FileSystemStateStorageHelper<>(HighAvailabilityServicesUtils
				.getClusterHighAvailableStoragePath(configuration), SUBMITTED_JOBGRAPH_FILE_PREFIX);
		return new KubernetesJobGraphStore(
			kubeClient,
			clusterId + NAME_SEPARATOR + jobGraphStoreName,
			new KubernetesStateHandleStore<>(kubeClient, stateStorage));
	}

	@Override
	public RunningJobsRegistry getRunningJobsRegistry() {
		return runningJobsRegistry;
	}

	@Override
	public BlobStore createBlobStore() {
		return blobStoreService;
	}

	@Override
	public void close() throws Exception {
		Throwable exception = null;

		try {
			blobStoreService.close();
		} catch (Throwable t) {
			exception = t;
		}

		kubeClient.close();

		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Could not properly close the KubernetesHaServices.");
		}
	}

	@Override
	public void closeAndCleanupAllData() throws Exception {
		LOG.info("Close and clean up all data for KubernetesHaServices.");

		Throwable exception = null;

		try {
			blobStoreService.closeAndCleanupAllData();
		} catch (Throwable t) {
			exception = t;
		}

		try {
			kubeClient.deleteConfigMapsByLabels(
				KubernetesUtils.getConfigMapLabels(clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
		} catch (Throwable t) {
			exception = ExceptionUtils.firstOrSuppressed(t, exception);
		}

		kubeClient.close();

		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Could not properly close and clean up all data of KubernetesHaServices.");
		}
	}

	private KubernetesLeaderElectionService createLeaderElectionService(String leaderName) {
		return new KubernetesLeaderElectionService(
			kubeClient,
			executor,
			KubernetesLeaderElectionConfiguration.fromConfiguration(getLeaderConfigMapName(leaderName), configuration));
	}

	private KubernetesLeaderRetrievalService createLeaderRetrievalService(String leaderName) {
		return new KubernetesLeaderRetrievalService(kubeClient, getLeaderConfigMapName(leaderName));
	}

	private String getLeaderConfigMapName(String leaderName) {
		return clusterId + NAME_SEPARATOR + leaderName + NAME_SEPARATOR + leaderSuffix;
	}

	private static String getLeaderNameForJobManager(final JobID jobID) {
		return jobID.toString() + NAME_SEPARATOR + JOB_MANAGER_LEADER_NAME;
	}
}

