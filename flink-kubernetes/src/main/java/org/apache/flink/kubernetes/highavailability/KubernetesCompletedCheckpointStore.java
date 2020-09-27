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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CompletedCheckpointStore} for JobManagers running in Kubernetes high availability.
 */
public class KubernetesCompletedCheckpointStore implements CompletedCheckpointStore {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesCompletedCheckpointStore.class);

	private static final Comparator<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> STRING_COMPARATOR = Comparator.comparing(o -> o.f1);

	private final KubernetesStateHandleStore<CompletedCheckpoint> checkpointsInKubernetes;

	/** The maximum number of checkpoints to retain (at least 1). */
	private final int maxNumberOfCheckpointsToRetain;

	/**
	 * Local copy of the completed checkpoints in Kubernetes. This is restored from Kubernetes
	 * when recovering and is maintained in parallel to the state in Kubernetes during normal
	 * operations.
	 */
	private final ArrayDeque<CompletedCheckpoint> completedCheckpoints;

	private final FlinkKubeClient kubeClient;

	private final Executor executor;

	private final String configMapName;

	/**
	 * Creates {@link KubernetesCompletedCheckpointStore}.
	 *
	 * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain (at
	 *                                       least 1). Adding more checkpoints than this results
	 *                                       in older checkpoints being discarded.
	 */
	public KubernetesCompletedCheckpointStore(
			FlinkKubeClient kubeClient,
			String configMapName,
			int maxNumberOfCheckpointsToRetain,
			KubernetesStateHandleStore<CompletedCheckpoint> checkpointsInKubernetes,
			Executor executor) {
		checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");
		this.kubeClient = kubeClient;
		this.configMapName = configMapName;
		this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;
		this.checkpointsInKubernetes = checkpointsInKubernetes;
		this.completedCheckpoints = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);
		this.executor = checkNotNull(executor);
	}

	@Override
	public boolean requiresExternalizedCheckpoints() {
		return true;
	}

	@Override
	public void recover() throws Exception {
		LOG.info("Recovering checkpoints from Kubernetes.");
		// Create the ConfigMap if not exist.
		kubeClient.createConfigMap(configMapName, new HashMap<>(), LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY);

		// Get all there is first
		List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> initialCheckpoints;
		while (true) {
			try {
				initialCheckpoints = checkpointsInKubernetes.getAll(configMapName);
				break;
			}
			catch (ConcurrentModificationException e) {
				LOG.warn("Concurrent modification while reading from Kubernetes. Retrying.");
			}
		}

		// TODO: needs to refactor, too much code duplication

		Collections.sort(initialCheckpoints, STRING_COMPARATOR);

		int numberOfInitialCheckpoints = initialCheckpoints.size();

		LOG.info("Found {} checkpoints in Kubernetes.", numberOfInitialCheckpoints);

		// Try and read the state handles from storage. We try until we either successfully read
		// all of them or when we reach a stable state, i.e. when we successfully read the same set
		// of checkpoints in two tries. We do it like this to protect against transient outages
		// of the checkpoint store (for example a DFS): if the DFS comes online midway through
		// reading a set of checkpoints we would run the risk of reading only a partial set
		// of checkpoints while we could in fact read the other checkpoints as well if we retried.
		// Waiting until a stable state protects against this while also being resilient against
		// checkpoints being actually unreadable.
		//
		// These considerations are also important in the scope of incremental checkpoints, where
		// we use ref-counting for shared state handles and might accidentally delete shared state
		// of checkpoints that we don't read due to transient storage outages.
		List<CompletedCheckpoint> lastTryRetrievedCheckpoints = new ArrayList<>(numberOfInitialCheckpoints);
		List<CompletedCheckpoint> retrievedCheckpoints = new ArrayList<>(numberOfInitialCheckpoints);
		do {
			LOG.info("Trying to fetch {} checkpoints from storage.", numberOfInitialCheckpoints);

			lastTryRetrievedCheckpoints.clear();
			lastTryRetrievedCheckpoints.addAll(retrievedCheckpoints);

			retrievedCheckpoints.clear();

			for (Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> checkpointStateHandle : initialCheckpoints) {

				CompletedCheckpoint completedCheckpoint = null;

				try {
					completedCheckpoint = retrieveCompletedCheckpoint(checkpointStateHandle);
					if (completedCheckpoint != null) {
						retrievedCheckpoints.add(completedCheckpoint);
					}
				} catch (Exception e) {
					LOG.warn("Could not retrieve checkpoint, not adding to list of recovered checkpoints.", e);
				}
			}

		} while (retrievedCheckpoints.size() != numberOfInitialCheckpoints &&
			!CompletedCheckpoint.checkpointsMatch(lastTryRetrievedCheckpoints, retrievedCheckpoints));

		// Clear local handles in order to prevent duplicates on
		// recovery. The local handles should reflect the state
		// of Kubernetes.
		completedCheckpoints.clear();
		completedCheckpoints.addAll(retrievedCheckpoints);

		if (completedCheckpoints.isEmpty() && numberOfInitialCheckpoints > 0) {
			throw new FlinkException(
				"Could not read any of the " + numberOfInitialCheckpoints + " checkpoints from storage.");
		} else if (completedCheckpoints.size() != numberOfInitialCheckpoints) {
			LOG.warn(
				"Could only fetch {} of {} checkpoints from storage.",
				completedCheckpoints.size(),
				numberOfInitialCheckpoints);
		}
	}

	@Override
	public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
		checkNotNull(checkpoint, "Checkpoint");

		final String key = checkpointIdToKey(checkpoint.getCheckpointID());

		// Now add the new one. If it fails, we don't want to loose existing data.
		checkpointsInKubernetes.add(configMapName, key, checkpoint);

		completedCheckpoints.addLast(checkpoint);

		// Everything worked, let's remove a previous checkpoint if necessary.
		while (completedCheckpoints.size() > maxNumberOfCheckpointsToRetain) {
			final CompletedCheckpoint completedCheckpoint = completedCheckpoints.removeFirst();
			tryRemoveCompletedCheckpoint(completedCheckpoint, CompletedCheckpoint::discardOnSubsume);
		}

		LOG.debug("Added {} to {}.", checkpoint, key);
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
		return new ArrayList<>(completedCheckpoints);
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		return completedCheckpoints.size();
	}

	@Override
	public int getMaxNumberOfRetainedCheckpoints() {
		return maxNumberOfCheckpointsToRetain;
	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		if (jobStatus.isGloballyTerminalState()) {
			LOG.info("Shutting down");

			for (CompletedCheckpoint checkpoint : completedCheckpoints) {
				tryRemoveCompletedCheckpoint(
					checkpoint,
					completedCheckpoint -> completedCheckpoint.discardOnShutdown(jobStatus));
			}

			completedCheckpoints.clear();
		} else {
			LOG.info("Suspending");

			// Clear the local handles, but don't remove any state
			completedCheckpoints.clear();
		}
	}

	private void tryRemoveCompletedCheckpoint(CompletedCheckpoint completedCheckpoint, ThrowingConsumer<CompletedCheckpoint, Exception> discardCallback) {
		try {
			if (tryRemove(completedCheckpoint.getCheckpointID())) {
				executor.execute(() -> {
					try {
						discardCallback.accept(completedCheckpoint);
					} catch (Exception e) {
						LOG.warn("Could not discard completed checkpoint {}.", completedCheckpoint.getCheckpointID(), e);
					}
				});

			}
		} catch (Exception e) {
			LOG.warn("Failed to subsume the old checkpoint", e);
		}
	}

	private boolean tryRemove(long checkpointId) throws Exception {
		return checkpointsInKubernetes.remove(configMapName, checkpointIdToKey(checkpointId));
	}

	private static String checkpointIdToKey(long checkpointId) {
		return String.format("%019d", checkpointId);
	}

	private static long keyToCheckpointId(String key) {
		try {
			return Long.parseLong(key);
		} catch (NumberFormatException e) {
			LOG.warn("Could not parse checkpoint id from {}. This indicates that the " +
				"checkpoint id to path conversion has changed.", key);

			return -1L;
		}
	}

	private static CompletedCheckpoint retrieveCompletedCheckpoint(
		Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> stateHandlePath) throws FlinkException {
		long checkpointId = keyToCheckpointId(stateHandlePath.f1);

		LOG.info("Trying to retrieve checkpoint {}.", checkpointId);

		try {
			return stateHandlePath.f0.retrieveState();
		} catch (ClassNotFoundException cnfe) {
			throw new FlinkException("Could not retrieve checkpoint " + checkpointId + " from state handle under " +
				stateHandlePath.f1 + ". This indicates that you are trying to recover from state written by an " +
				"older Flink version which is not compatible. Try cleaning the state handle store.", cnfe);
		} catch (IOException ioe) {
			throw new FlinkException("Could not retrieve checkpoint " + checkpointId + " from state handle under " +
				stateHandlePath.f1 + ". This indicates that the retrieved state handle is broken. Try cleaning the " +
				"state handle store.", ioe);
		}
	}
}
