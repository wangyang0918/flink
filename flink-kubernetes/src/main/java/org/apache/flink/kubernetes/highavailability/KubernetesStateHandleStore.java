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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Class which stores state via the provided {@link RetrievableStateStorageHelper} and writes the
 * returned state handle to ConfigMap.
 *
 * <p>Added state is persisted via {@link RetrievableStateHandle RetrievableStateHandles},
 * which in turn are written to ConfigMap. This level of indirection is necessary to keep the
 * amount of data in ConfigMap small. ConfigMap is build for data in the KB range whereas
 * state can grow to multiple MBs.
 *
 * <p>State modifications require some care, because it is possible that certain failures bring
 * the state handle backend and ConfigMap out of sync.
 *
 * @param <T> Type of state
 */

// TODO support lock and release to avoid concurrent deletion.
public class KubernetesStateHandleStore<T extends Serializable> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesStateHandleStore.class);

	private final FlinkKubeClient kubeClient;

	private final RetrievableStateStorageHelper<T> storage;

	/**
	 * Creates a {@link KubernetesStateHandleStore}.
	 *
	 * @param kubeClient    The Kubernetes client.
	 * @param storage       To persist the actual state and whose returned state handle is then written to ConfigMap
	 */
	public KubernetesStateHandleStore(FlinkKubeClient kubeClient, RetrievableStateStorageHelper<T> storage) {

		this.kubeClient = checkNotNull(kubeClient, "Kubernetes client");
		this.storage = checkNotNull(storage, "State storage");
	}

	/**
	 * Creates a state handle, stores it in ConfigMap.
	 *
	 * @param configMapName   ConfigMap name
	 * @param key             Key
	 * @param state           State to be added
	 *
	 * @return The Created {@link RetrievableStateHandle}.
	 * @throws Exception If a ConfigMap or state handle operation fails
	 */
	public RetrievableStateHandle<T> add(String configMapName, String key, T state) throws Exception {
		checkNotNull(key, "Key in ConfigMap");
		checkNotNull(state, "State");

		RetrievableStateHandle<T> storeHandle = storage.store(state);

		boolean success = false;

		try {
			// Serialize the state handle. This writes the state to the backend.
			byte[] serializedStoreHandle = InstantiationUtil.serializeObject(storeHandle);

			kubeClient.getConfigMap(configMapName).ifPresent(
				FunctionUtils.uncheckedConsumer(
					configMap -> {
						final Map<String, String> data = new HashMap<>();
						if (configMap.getData() != null) {
							data.putAll(configMap.getData());
						}
						data.put(key, Base64.getEncoder().encodeToString(serializedStoreHandle));
						configMap.setData(data);
						kubeClient.updateConfigMap(configMap).get();
					})
			);
			success = true;
			return storeHandle;
		} finally {
			if (!success) {
				// Cleanup the state handle if it was not written to ConfigMap.
				if (storeHandle != null) {
					storeHandle.discardState();
				}
			}
		}
	}

	/**
	 * Replaces a state handle in ConfigMap and discards the old state handle.
	 *
	 * @param configMapName   ConfigMap name
	 * @param key             Key
	 * @param state           State to be added
	 *
	 * @throws Exception If a ConfigMap or state handle operation fails
	 */
	public void replace(String configMapName, String key, T state) throws Exception {
		checkNotNull(key, "Key in ConfigMap");
		checkNotNull(state, "State");

		RetrievableStateHandle<T> oldStateHandle = get(configMapName, key);

		RetrievableStateHandle<T> newStateHandle = storage.store(state);

		boolean success = false;

		try {
			// Serialize the state handle. This writes the state to the backend.
			byte[] serializedStoreHandle = InstantiationUtil.serializeObject(state);

			kubeClient.getConfigMap(configMapName).ifPresent(
				FunctionUtils.uncheckedConsumer(
					configMap -> {
						final Map<String, String> data = new HashMap<>();
						if (configMap.getData() != null) {
							data.putAll(configMap.getData());
						}
						data.put(key, Base64.getEncoder().encodeToString(serializedStoreHandle));
						configMap.setData(data);
						kubeClient.updateConfigMap(configMap).get();
					})
			);

			success = true;
		} finally {
			if (success) {
				oldStateHandle.discardState();
			} else {
				newStateHandle.discardState();
			}
		}

	}

	/**
	 * Returns the key in config map exists or not.
	 *
	 * @param configMapName   ConfigMap name
	 * @param key             Key
	 *
	 * @return true or false
	 */
	public boolean exists(String configMapName, String key) {
		checkNotNull(configMapName, "Key in ConfigMap");
		checkNotNull(key, "State");

		final Optional<KubernetesConfigMap> optional = kubeClient.getConfigMap(configMapName);

		return optional.isPresent() && optional.get().getData() != null && optional.get().getData().containsKey(key);
	}

	/**
	 * Gets the {@link RetrievableStateHandle} stored in the given ConfigMap.
	 *
	 * @param configMapName   ConfigMap name
	 * @param key             Key
	 *
	 * @return The retrieved state handle from the specified ConfigMap
	 * @throws IOException Thrown if the method failed to deserialize the stored state handle
	 * @throws Exception Thrown if a ConfigMap operation failed
	 */
	public RetrievableStateHandle<T> get(String configMapName, String key) throws Exception {
		checkNotNull(configMapName, "Key in ConfigMap");
		checkNotNull(key, "State");

		final Optional<KubernetesConfigMap> optional = kubeClient.getConfigMap(configMapName);
		checkState(optional.isPresent(), "ConfigMap %s do not exist.", configMapName);
		final KubernetesConfigMap configMap = optional.get();
		if (configMap.getData() == null || !configMap.getData().containsKey(key)) {
			throw new Exception(key + " not exit in ConfigMap " + configMapName);
		}
		return deserializeObject(optional.get().getData().get(key));
	}

	/**
	 * Return a list of all valid paths for state handles.
	 *
	 * @param configMapName   ConfigMap name
	 *
	 * @return List of valid state handle keys in Kubernetes ConfigMap
	 */
	public Collection<String> getAllKeys(String configMapName) {
		checkNotNull(configMapName, "Key in ConfigMap");

		final Optional<KubernetesConfigMap> optional = kubeClient.getConfigMap(configMapName);
		if (optional.isPresent() && optional.get().getData() != null) {
			return optional.get().getData().keySet();
		} else {
			return Collections.emptySet();
		}
	}

	/**
	 * Remove the key in state config map.
	 * It returns the {@link RetrievableStateHandle} stored under the given state node if any.
	 *
	 * @param configMapName   ConfigMap name
	 * @param key             Key
	 *
	 * @return True if the state handle could be released
	 * @throws Exception If the ConfigMap operation or discarding the state handle fails
	 */
	public boolean remove(String configMapName, String key) throws Exception {
		checkNotNull(configMapName, "Key in ConfigMap");
		checkNotNull(key, "State");

		RetrievableStateHandle<T> stateHandle = null;

		try {
			stateHandle = get(configMapName, key);
		} catch (Exception e) {
			LOG.warn("Could not retrieve the state handle of {} from ConfigMap {}.", key, configMapName, e);
		}

		try {
			kubeClient.getConfigMap(configMapName).ifPresent(
				FunctionUtils.uncheckedConsumer(
					configMap -> {
						configMap.getData().remove(key);
						kubeClient.updateConfigMap(configMap).get();
					})
			);
		} catch (Exception e) {
			LOG.debug("Failed to remove the key {} in {}.", key, configMapName);
			return false;
		}

		if (stateHandle != null) {
			stateHandle.discardState();
		}

		return true;
	}

	/**
	 * Gets all available state handles from Kubernetes and locks the respective state nodes.
	 *
	 * <p>If there is a concurrent modification, the operation is retried until it succeeds.
	 *
	 * @return All state handles from ConfigMap.
	 * @throws Exception If a ConfigMap or state handle operation fails
	 */
	@SuppressWarnings("unchecked")
	public List<Tuple2<RetrievableStateHandle<T>, String>> getAll(String configMapName) throws Exception {
		final List<Tuple2<RetrievableStateHandle<T>, String>> stateHandles = new ArrayList<>();

		checkNotNull(configMapName, "Key in ConfigMap");

		final Optional<KubernetesConfigMap> optional = kubeClient.getConfigMap(configMapName);

		if (optional.isPresent() && optional.get().getData() != null) {
			for (Map.Entry<String, String> entrySet : optional.get().getData().entrySet()) {
				stateHandles.add(new Tuple2(deserializeObject(entrySet.getValue()), entrySet.getKey()));
			}
		}

		return stateHandles;
	}

	private RetrievableStateHandle<T> deserializeObject(String content) throws Exception {
		checkNotNull(content, "Content should not be null.");

		byte[] data = Base64.getDecoder().decode(content);

		try {
			return InstantiationUtil.deserializeObject(data, Thread.currentThread().getContextClassLoader());
		} catch (IOException | ClassNotFoundException e) {
			throw new IOException("Failed to deserialize state handle from ConfigMap data " +
				content + '.', e);
		}
	}
}
