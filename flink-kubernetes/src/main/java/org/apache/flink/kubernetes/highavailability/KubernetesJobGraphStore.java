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
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link JobGraph} instances for JobManagers running in {@link HighAvailabilityMode}.
 * All the jobs running in a same Flink cluster will share a ConfigMap to store the job graphs.
 * The ConfigMap is watched to detect concurrent modifications in corner situations where
 * multiple instances operate concurrently. The job manager acts as a {@link JobGraphListener}
 * to react to such situations.
 */
public class KubernetesJobGraphStore implements JobGraphStore {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesJobGraphStore.class);

	/** Lock to synchronize with the {@link JobGraphListener}. */
	private final Object lock = new Object();

	private final FlinkKubeClient kubeClient;

	/** The set of IDs of all added job graphs. */
	private final Set<JobID> addedJobGraphs = new HashSet<>();

	/** Submitted job graphs in Kubernetes ConfigMap. */
	private final KubernetesStateHandleStore<JobGraph> jobGraphsInKubernetes;

	/** The external listener to be notified on races. */
	private JobGraphListener jobGraphListener;

	/** Flag indicating whether this instance is running. */
	private boolean isRunning;

	private final String configMapName;

	private final int maxRetryAttempts;

	private KubernetesWatch kubernetesWatch;

	public KubernetesJobGraphStore(
			FlinkKubeClient kubeClient,
			String configMapName,
			int maxRetryAttempts,
			KubernetesStateHandleStore<JobGraph> jobGraphsInKubernetes) {
		this.kubeClient = checkNotNull(kubeClient);
		this.configMapName = checkNotNull(configMapName);
		this.maxRetryAttempts = checkNotNull(maxRetryAttempts);
		this.jobGraphsInKubernetes = checkNotNull(jobGraphsInKubernetes);
	}

	@Override
	public void start(JobGraphListener jobGraphListener) {
		synchronized (lock) {
			if (!isRunning) {
				this.jobGraphListener = jobGraphListener;
				kubeClient.createConfigMap(configMapName, new HashMap<>(), LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY);
				kubernetesWatch = kubeClient.watchConfigMapsAndDoCallback(
					configMapName, new ConfigMapCallbackHandlerImpl());
				isRunning = true;
			}
		}
	}

	@Override
	public void stop() {
		synchronized (lock) {
			if (isRunning) {
				isRunning = false;
				if (kubernetesWatch != null) {
					kubernetesWatch.close();
				}
			}
		}
	}

	@Nullable
	@Override
	public JobGraph recoverJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");
		final String key = jobId.toString();

		LOG.debug("Recovering job graph {} from {}.", jobId, configMapName);

		synchronized (lock) {
			checkState(isRunning, "Not running. Forgot to call start()?");

			boolean success = false;

			try {
				final RetrievableStateHandle<JobGraph> jobGraphRetrievableStateHandle;

				try {
					jobGraphRetrievableStateHandle = jobGraphsInKubernetes.getAndLock(key);
				} catch (Exception e) {
					throw new FlinkException("Could not retrieve the submitted job graph state handle " +
						"for " + key + " from the submitted job graph store.", e);
				}
				final JobGraph jobGraph;

				try {
					jobGraph = jobGraphRetrievableStateHandle.retrieveState();
				} catch (ClassNotFoundException cnfe) {
					throw new FlinkException("Could not retrieve submitted JobGraph from state handle under " +
						key + " in " + configMapName +
						". This indicates that you are trying to recover from state written by an " +
						"older Flink version which is not compatible. Try cleaning the state handle store.", cnfe);
				} catch (IOException ioe) {
					throw new FlinkException("Could not retrieve submitted JobGraph from state handle under " +
						key + " in " + configMapName +
						". This indicates that the retrieved state handle is broken. Try cleaning the state handle " +
						"store.", ioe);
				}

				addedJobGraphs.add(jobGraph.getJobID());

				LOG.info("Recovered {}.", jobGraph);

				success = true;
				return jobGraph;
			} finally {
				if (!success) {
					jobGraphsInKubernetes.releaseAll();
				}
			}
		}
	}

	@Override
	public Collection<JobID> getJobIds() throws Exception {
		Collection<String> keys;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Retrieving all stored job ids from Kubernetes ConfigMap {}.", configMapName);
		}

		try {
			keys = jobGraphsInKubernetes.getAllKeys();
		} catch (Exception e) {
			throw new Exception("Failed to retrieve entry paths from KubernetesStateHandleStore.", e);
		}

		final List<JobID> jobIds = new ArrayList<>(keys.size());

		for (String key : keys) {
			try {
				jobIds.add(JobID.fromHexString(key));
			} catch (Exception exception) {
				LOG.warn("Could not parse job id from {}. This indicates a malformed key.", key, exception);
			}
		}

		return jobIds;
	}

	@Override
	public void putJobGraph(JobGraph jobGraph) throws Exception {
		checkNotNull(jobGraph, "Job graph");

		LOG.debug("Adding job graph {} to {}.", jobGraph.getJobID(), configMapName);

		boolean success = false;
		final String jobId = jobGraph.getJobID().toString();

		while (!success && isRunning) {
			synchronized (lock) {
				try {
					if (!jobGraphsInKubernetes.exists(jobId)) {
						jobGraphsInKubernetes.addAndLock(jobGraph.getJobID().toString(), jobGraph);
						addedJobGraphs.add(jobGraph.getJobID());
					} else if (addedJobGraphs.contains(jobGraph.getJobID())) {
						jobGraphsInKubernetes.replace(jobGraph.getJobID().toString(), jobGraph);
						LOG.info("Updated {} in config map.", jobGraph);
					} else {
						throw new IllegalStateException("Oh, no. Trying to update a graph you didn't " +
							"#getAllSubmittedJobGraphs() or #putJobGraph() yourself before.");
					}
					success = true;
				} catch (Exception e) {
					LOG.debug("Could not update the config map {}, {}", configMapName, e);
				}
			}
		}

		LOG.info("Added {} to ConfigMap.", jobGraph);
	}

	@Override
	public void removeJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");

		LOG.debug("Removing job graph {} from {}.", jobId, configMapName);

		synchronized (lock) {
			if (addedJobGraphs.contains(jobId)) {
				if (jobGraphsInKubernetes.releaseAndTryRemove(jobId.toString())) {
					addedJobGraphs.remove(jobId);
				} else {
					throw new FlinkException(String.format("Could not remove job graph with job id %s from ConfigMap.", jobId));
				}
			}
		}

		LOG.info("Removed job graph {} from ConfigMap {}.", jobId, configMapName);
	}

	@Override
	public void releaseJobGraph(JobID jobId) throws Exception {
		checkNotNull(jobId, "Job ID");

		LOG.debug("Releasing job graph {} from {}.", jobId, configMapName);

		synchronized (lock) {
			if (addedJobGraphs.contains(jobId)) {
				jobGraphsInKubernetes.release(jobId.toString());

				addedJobGraphs.remove(jobId);
			}
		}

		LOG.info("Released locks of job graph {} from ConfigMap {}.", jobId, configMapName);
	}

	private class ConfigMapCallbackHandlerImpl implements FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> {
		@Override
		public void onAdded(List<KubernetesConfigMap> configMaps) {
			configMaps.forEach(configMap -> handleConfigMapChanged(getJobIDs(configMap)));
		}

		@Override
		public void onModified(List<KubernetesConfigMap> configMaps) {
			configMaps.forEach(configMap -> handleConfigMapChanged(getJobIDs(configMap)));
		}

		@Override
		public void onDeleted(List<KubernetesConfigMap> configMaps) {
			handleConfigMapChanged(Collections.emptySet());
		}

		@Override
		public void onError(List<KubernetesConfigMap> configMaps) {
			LOG.error("Error while watching the configMap {}", configMapName);
		}

		@Override
		public void handleFatalError(Throwable throwable) {
			LOG.error("Error while watching the configMap {}", configMapName, throwable);
		}

		private Set<JobID> getJobIDs(KubernetesConfigMap configMap) {
			final Set<JobID> jobIDs;
			if (configMap.getData() == null) {
				jobIDs = Collections.emptySet();
			} else {
				jobIDs = configMap.getData().keySet().stream().map(JobID::fromHexString).collect(Collectors.toSet());
			}
			return jobIDs;
		}

		private void handleConfigMapChanged(Set<JobID> jobIDs) {
			synchronized (lock) {
				final Set<JobID> jobIDsToAdd = new HashSet<>();
				final Set<JobID> jobIDsToRemove = new HashSet<>(addedJobGraphs);

				jobIDs.forEach(jobID -> {
					if (!jobIDsToRemove.remove(jobID)) {
						jobIDsToAdd.add(jobID);
					}
				});

				for (JobID jobId : jobIDsToRemove) {
					try {
						if (jobGraphListener != null && addedJobGraphs.contains(jobId)) {
							try {
								jobGraphListener.onRemovedJobGraph(jobId);
							} catch (Throwable t) {
								LOG.error("Error in callback", t);
							}
						}

						break;
					} catch (Exception e) {
						LOG.error("Error in SubmittedJobGraphsPathCacheListener", e);
					}
				}

				for (JobID jobId : jobIDsToAdd) {
					try {
						if (jobGraphListener != null && !addedJobGraphs.contains(jobId)) {
							try {
								jobGraphListener.onAddedJobGraph(jobId);
							} catch (Throwable t) {
								LOG.error("Error in callback", t);
							}
						}
					} catch (Exception e) {
						LOG.error("Error in SubmittedJobGraphsPathCacheListener", e);
					}
				}
			}
		}
	}
}
