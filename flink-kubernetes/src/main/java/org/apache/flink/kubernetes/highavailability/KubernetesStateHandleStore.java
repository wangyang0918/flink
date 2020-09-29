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
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
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

	private static final String KEY_LOCK_SUFFIX = "lock";

	private static final String LOCK_IDENTITY = UUID.randomUUID().toString();

	private final FlinkKubeClient kubeClient;

	private final Executor executor;

	private final String configMapName;

	private final RetrievableStateStorageHelper<T> storage;

	private final int maxRetryAttempts;

	private final Supplier<FlinkRuntimeException> configMapNotExistOrEmptySupplier;

	/**
	 * Creates a {@link KubernetesStateHandleStore}.
	 *
	 * @param kubeClient    The Kubernetes client.
	 * @param storage       To persist the actual state and whose returned state handle is then written to ConfigMap
	 * @param maxRetryAttempts Max retry attempts to update the Kubernetes ConfigMap
	 */
	public KubernetesStateHandleStore(
			FlinkKubeClient kubeClient,
			Executor executor,
			String configMapName,
			RetrievableStateStorageHelper<T> storage,
			int maxRetryAttempts) {

		this.kubeClient = checkNotNull(kubeClient, "Kubernetes client");
		this.storage = checkNotNull(storage, "State storage");
		this.executor = checkNotNull(executor, "Executor");
		this.configMapName = checkNotNull(configMapName, "ConfigMap name");
		this.maxRetryAttempts = maxRetryAttempts;

		this.configMapNotExistOrEmptySupplier =
			() -> new FlinkRuntimeException("ConfigMap " + configMapName + " not exists or is empty");
		LOG.info("Creating KubernetesStateHandleStore with lock identity {}.", LOCK_IDENTITY);
	}

	/**
	 * Creates a state handle, stores it in ConfigMap. Lock means adding a key with suffix {@link KubernetesStateHandleStore#KEY_LOCK_SUFFIX}.
	 * Only the "lock" key is empty, the corresponding key could be deleted.
	 *
	 * @param key             Key
	 * @param state           State to be added
	 *
	 * @throws Exception If a ConfigMap or state handle operation fails
	 */
	public void addAndLock(String key, T state) throws Exception {
		checkNotNull(key, "Key in ConfigMap.");
		checkNotNull(state, "State.");

		final RetrievableStateHandle<T> storeHandle = storage.store(state);

		boolean success = false;

		try {
			add(key, storeHandle);
			success = true;
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
			add(key, newStateHandle);
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
	 * @param key             Key
	 *
	 * @return true or false
	 */
	public boolean exists(String key) {
		checkNotNull(key, "Key in ConfigMap.");

		final Optional<KubernetesConfigMap> optional = kubeClient.getConfigMap(configMapName);

		return optional.isPresent() && optional.get().getData() != null && optional.get().getData().containsKey(key);
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
	public RetrievableStateHandle<T> getAndLock(String key) throws Exception {
		checkNotNull(key, "Key in ConfigMap.");

		return retry(() -> kubeClient.getConfigMap(configMapName)
			.filter(configMap -> configMap.getData() != null && configMap.getData().containsKey(key))
			.map(
				FunctionUtils.uncheckedFunction(
					configMap -> {
						final RetrievableStateHandle<T> stateHandle = deserializeObject(configMap.getData().get(key));
						configMap.getData().put(getKeyForLock(key), LOCK_IDENTITY);
						kubeClient.updateConfigMap(configMap).get();
						return stateHandle;
					}
				))
			.orElseThrow(configMapNotExistOrEmptySupplier)).get();
	}

	/**
	 * Remove the key in state config map.
	 * It returns the {@link RetrievableStateHandle} stored under the given state node if any.
	 *
	 * @param key             Key
	 *
	 * @return True if the state handle could be released
	 */
	public boolean releaseAndTryRemove(String key) {
		checkNotNull(key, "Key in ConfigMap.");

		try {
			return retry(() -> kubeClient.getConfigMap(configMapName)
				.filter(configMap -> configMap.getData() != null && configMap.getData().containsKey(key))
				.map(
					FunctionUtils.uncheckedFunction(
						configMap -> {
							final String identity = configMap.getData().remove(getKeyForLock(key));
							// Check the owner of key
							if (identity == null || LOCK_IDENTITY.equals(identity)) {
								final String content = configMap.getData().remove(key);
								try {
									final RetrievableStateHandle<T> stateHandle = deserializeObject(content);
									stateHandle.discardState();
								} catch (Exception e) {
									LOG.warn("Could not retrieve the state handle of {} from ConfigMap {}.",
										key, configMapName, e);
								}
								kubeClient.updateConfigMap(configMap).get();
							} else {
								LOG.debug("Could not remove {} in {}. The ownership is {}, current is {}.",
									key, configMapName, identity, LOCK_IDENTITY);
							}
							return true;
						})
				)
				.orElse(true)).get();
		} catch (Exception e) {
			LOG.debug("Failed to remove the key {} in {}.", key, configMapName);
			return false;
		}
	}

	/**
	 * Gets all available state handles from Kubernetes and locks the respective state nodes.
	 *
	 * <p>If there is a concurrent modification, the operation is retried until it succeeds.
	 *
	 * @return All state handles from ConfigMap.
	 */
	@SuppressWarnings("unchecked")
	public List<Tuple2<RetrievableStateHandle<T>, String>> getAllAndLock() throws Exception {

		final List<Tuple2<RetrievableStateHandle<T>, String>> stateHandles = new ArrayList<>();
		return retry(() -> kubeClient.getConfigMap(configMapName)
			.filter(configMap -> configMap.getData() != null)
			.map(
				FunctionUtils.uncheckedFunction(
					configMap -> {
						final Map<String, String> data = new HashMap<>(configMap.getData());
						configMap.getData().entrySet().stream()
							.filter(entry -> !entry.getKey().endsWith(KEY_LOCK_SUFFIX)) // Not the lock key
							.forEach(
								entry -> {
									try {
										stateHandles.add(
											new Tuple2(deserializeObject(entry.getValue()), entry.getKey()));
										// Update the lock
										data.put(getKeyForLock(entry.getKey()), LOCK_IDENTITY);
									} catch (Exception e) {
										LOG.warn("ConfigMap {} contained corrupted data. Ignoring the key {}.",
											configMapName, entry.getKey());
									}
								}
							);
						configMap.setData(data);
						kubeClient.updateConfigMap(configMap).get();
						return stateHandles;
					}))
			.orElse(stateHandles)).get();
	}

	/**
	 * Return a list of all valid paths for state handles.
	 *
	 *
	 * @return List of valid state handle keys in Kubernetes ConfigMap
	 */
	public Collection<String> getAllKeys() {

		final List<String> keys = new ArrayList<>();
		kubeClient.getConfigMap(configMapName)
			.filter(configMap -> configMap.getData() != null)
			.ifPresent(
				configMap -> keys.addAll(
					configMap.getData().keySet().stream()
						.filter(key -> !key.endsWith(KEY_LOCK_SUFFIX))
						.collect(Collectors.toList())));
		return keys;
	}

	/**
	 * Releases the lock from the key in the given ConfigMap. If no lock exists, then nothing happens.
	 *
	 * @param key             Key to release
	 *
	 * @throws Exception if the delete operation of the lock key fails
	 */
	public void release(String key) throws Exception {
		checkNotNull(key, "Key in ConfigMap.");

		try {
			retry(() -> kubeClient.getConfigMap(configMapName)
				.filter(configMap -> configMap.getData() != null)
				.map(
					FunctionUtils.uncheckedFunction(
						configMap -> {
							final String identity = configMap.getData().remove(getKeyForLock(key));
							if (identity == null || LOCK_IDENTITY.equals(identity)) {
								configMap.getData().remove(key);
								kubeClient.updateConfigMap(configMap).get();
							} else {
								LOG.debug("Could not release {} in {}. The ownership is {}, current is {}.",
									key, configMap, identity, LOCK_IDENTITY);
							}
							return true;
						})
				).orElse(true)).get();
		} catch (Exception e) {
			throw new Exception("Failed to release the key " + key + " in " + configMapName + ".");
		}
	}

	/**
	 * Releases all lock keys of this {@link KubernetesStateHandleStore}.
	 *
	 * @throws Exception if the delete operation of the lock key fails
	 */
	public void releaseAll() throws Exception {
		retry(() -> kubeClient.getConfigMap(configMapName)
			.filter(configMap -> configMap.getData() != null)
			.map(
				FunctionUtils.uncheckedFunction(
					configMap -> {
						final Map<String, String> data = new HashMap<>(configMap.getData());
						configMap.getData().forEach(
							(key, value) -> {
								if (value != null && value.equals(LOCK_IDENTITY)) {
									data.remove(key);
								}
							});
						configMap.setData(data);
						kubeClient.updateConfigMap(configMap).get();
						return true;
					})
			).orElse(true)).get();
	}

	private void add(String key, RetrievableStateHandle<T> stateHandle) throws Exception {
		// Serialize the state handle. This writes the state to the backend.
		byte[] serializedStoreHandle = InstantiationUtil.serializeObject(stateHandle);

		retry(() -> kubeClient.getConfigMap(configMapName)
			.map(
				FunctionUtils.uncheckedFunction(
					configMap -> {
						final Map<String, String> data = new HashMap<>();
						if (configMap.getData() != null) {
							data.putAll(configMap.getData());
						}
						data.put(key, Base64.getEncoder().encodeToString(serializedStoreHandle));
						data.put(getKeyForLock(key), LOCK_IDENTITY);
						configMap.setData(data);
						kubeClient.updateConfigMap(configMap).get();
						return true;
					})
			).orElse(true)).get();
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
			.filter(configMap -> configMap.getData() != null && configMap.getData().containsKey(key))
			.map(FunctionUtils.uncheckedFunction(configMap -> deserializeObject(configMap.getData().get(key))))
			.orElseThrow(() -> new Exception(key + " not exists in ConfigMap " + configMapName));
	}

	private String getKeyForLock(String key) {
		return key + Constants.NAME_SEPARATOR + KEY_LOCK_SUFFIX;
	}

	private <S> CompletableFuture<S> retry(final Supplier<S> operation) {
		return FutureUtils.retry(
			() -> CompletableFuture.supplyAsync(operation, executor),
			maxRetryAttempts,
			executor);
	}
}
