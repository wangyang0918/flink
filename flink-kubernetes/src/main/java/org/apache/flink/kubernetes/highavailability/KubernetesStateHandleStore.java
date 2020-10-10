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
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class which stores state via the provided {@link RetrievableStateStorageHelper} and writes the
 * returned state handle to ConfigMap.
 *
 * <p>Added state is persisted via {@link RetrievableStateHandle RetrievableStateHandles},
 * which in turn are written to ConfigMap. This level of indirection is necessary to keep the
 * amount of data in ConfigMap small. ConfigMap is build for data in the KB range whereas
 * state can grow to multiple MBs.
 *
 * <p>Since concurrent modification may happen on a same ConfigMap by different KubernetesStateHandleStore, all the
 * ConfigMap update operations need to be done with retry attempts. Max retry attempts could be configured via
 * {@link org.apache.flink.kubernetes.configuration.KubernetesHighAvailabilityOptions#KUBERNETES_MAX_RETRY_ATTEMPTS}
 *
 * @param <T> Type of state
 */
public class KubernetesStateHandleStore<T extends Serializable> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesStateHandleStore.class);

	private final FlinkKubeClient kubeClient;

	private final String configMapName;

	private final RetrievableStateStorageHelper<T> storage;

	/**
	 * Creates a {@link KubernetesStateHandleStore}.
	 *
	 * @param kubeClient    The Kubernetes client.
	 * @param storage       To persist the actual state and whose returned state handle is then written to ConfigMap
	 */
	public KubernetesStateHandleStore(
			FlinkKubeClient kubeClient,
			String configMapName,
			RetrievableStateStorageHelper<T> storage) {

		this.kubeClient = checkNotNull(kubeClient, "Kubernetes client");
		this.storage = checkNotNull(storage, "State storage");
		this.configMapName = checkNotNull(configMapName, "ConfigMap name");
	}

	/**
	 * Creates a state handle, stores it in ConfigMap. We could guarantee that only the leader could write the ConfigMap
	 * in a transactional operation. Since “Get(check the leader)-and-Update(write back to the ConfigMap)” is a
	 * transactional operation.
	 *
	 * @param key             Key
	 * @param state           State to be added
	 *
	 * @throws Exception If a ConfigMap or state handle operation fails
	 */
	public void add(String key, T state) throws Exception {
		checkNotNull(key, "Key in ConfigMap.");
		checkNotNull(state, "State.");

		final RetrievableStateHandle<T> storeHandle = storage.store(state);

		boolean success = false;

		try {
			success = add(key, storeHandle);
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
	 * @param key             Key
	 * @param state           State to be added
	 *
	 * @throws Exception If a ConfigMap or state handle operation fails
	 */
	public void replace(String key, T state) throws Exception {
		checkNotNull(key, "Key in ConfigMap.");
		checkNotNull(state, "State.");

		final RetrievableStateHandle<T> oldStateHandle = get(configMapName, key);

		final RetrievableStateHandle<T> newStateHandle = storage.store(state);

		boolean success = false;

		try {
			success = add(key, newStateHandle);
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
	 * @param key             Key
	 *
	 * @return true or false
	 */
	public boolean exists(String key) {
		checkNotNull(key, "Key in ConfigMap.");

		final Optional<KubernetesConfigMap> optional = kubeClient.getConfigMap(configMapName);

		return optional.isPresent() && optional.get().getData().containsKey(key);
	}

	/**
	 * Gets the {@link RetrievableStateHandle} stored in the given ConfigMap.
	 *
	 * @param key             Key
	 *
	 * @return The retrieved state handle from the specified ConfigMap
	 * @throws IOException Thrown if the method failed to deserialize the stored state handle
	 * @throws Exception Thrown if a ConfigMap operation failed
	 */
	public RetrievableStateHandle<T> get(String key) throws Exception {
		checkNotNull(key, "Key in ConfigMap.");

		final Optional<KubernetesConfigMap> optional = kubeClient.getConfigMap(configMapName);
		if (optional.isPresent()) {
			final KubernetesConfigMap configMap = optional.get();
			if (configMap.getData().containsKey(key)) {
				return deserializeObject(configMap.getData().get(key));
			} else {
				throw new FlinkException("Could not find " + key + " in ConfigMap " + configMapName);
			}
		} else {
			throw new FlinkException("ConfigMap " + configMapName + " not exists or is empty");
		}
	}

	/**
	 * Remove the key in state config map.
	 * It returns the {@link RetrievableStateHandle} stored under the given state node if any.
	 *
	 * @param key             Key
	 *
	 * @return True if the state handle could be released
	 */
	public boolean remove(String key) {
		checkNotNull(key, "Key in ConfigMap.");

		try {
			return kubeClient.checkAndUpdateConfigMap(
				configMapName,
				KubernetesUtils.getLeaderChecker(),
				configMap -> {
					if (configMap.getData().containsKey(key)) {
						final String content = configMap.getData().remove(key);
						try {
							final RetrievableStateHandle<T> stateHandle = deserializeObject(content);
							stateHandle.discardState();
						} catch (Exception e) {
							LOG.warn(
								"Could not retrieve the state handle of {} from ConfigMap {}.", key, configMapName, e);
						}
					}
					return configMap;
				}).get();
		} catch (Exception e) {
			LOG.debug("Failed to remove the key {} in {}.", key, configMapName);
			return false;
		}
	}

	/**
	 * Gets all available state handles from Kubernetes.
	 *
	 * <p>If there is a concurrent modification, the operation is retried until it succeeds.
	 *
	 * @return All state handles from ConfigMap.
	 */
	@SuppressWarnings("unchecked")
	public List<Tuple2<RetrievableStateHandle<T>, String>> getAll(Predicate<Map.Entry<String, String>> filter) {

		final List<Tuple2<RetrievableStateHandle<T>, String>> stateHandles = new ArrayList<>();
		return kubeClient.getConfigMap(configMapName)
			.map(
				configMap -> {
					configMap.getData().entrySet().stream()
						.filter(filter)
						.forEach(entry -> {
							try {
								stateHandles.add(new Tuple2(deserializeObject(entry.getValue()), entry.getKey()));
							} catch (Exception e) {
								LOG.warn("ConfigMap {} contained corrupted data. Ignoring the key {}.",
									configMapName, entry.getKey());
							}
						});
					return stateHandles;
				})
			.orElse(stateHandles);
	}

	/**
	 * Return a list of all valid paths for state handles.
	 *
	 *
	 * @return List of valid state handle keys in Kubernetes ConfigMap
	 */
	public Collection<String> getAllKeys(Predicate<String> filter) {

		final List<String> keys = new ArrayList<>();
		kubeClient.getConfigMap(configMapName)
			.ifPresent(configMap -> keys.addAll(
				configMap.getData().keySet().stream().filter(filter).collect(Collectors.toList())));
		return keys;
	}

	private boolean add(String key, RetrievableStateHandle<T> stateHandle) throws Exception {
		// Serialize the state handle. This writes the state to the backend.
		byte[] serializedStoreHandle = InstantiationUtil.serializeObject(stateHandle);

		return kubeClient.checkAndUpdateConfigMap(
			configMapName,
			KubernetesUtils.getLeaderChecker(),
			configMap -> {
				configMap.getData().put(key, Base64.getEncoder().encodeToString(serializedStoreHandle));
				return configMap;
			}).get();
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

	private RetrievableStateHandle<T> get(String configMapName, String key) throws Exception {
		checkNotNull(configMapName, "Key in ConfigMap");
		checkNotNull(key, "State");

		return kubeClient.getConfigMap(configMapName)
			.filter(configMap -> configMap.getData().containsKey(key))
			.map(FunctionUtils.uncheckedFunction(configMap -> deserializeObject(configMap.getData().get(key))))
			.orElseThrow(() -> new Exception(key + " not exists in ConfigMap " + configMapName));
	}
}
