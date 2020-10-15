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
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.flink.kubernetes.utils.Constants.CHECKPOINT_COUNTER_KEY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CheckpointIDCounter} instances for JobManagers running in Kubernetes high availability.
 */
public class KubernetesCheckpointIDCounter implements CheckpointIDCounter {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesCheckpointIDCounter.class);

	private final Object lock = new Object();

	private final FlinkKubeClient kubeClient;

	private final String configMapName;

	private volatile boolean running;

	private final Supplier<FlinkRuntimeException> configMapNotExistSupplier;

	public KubernetesCheckpointIDCounter(FlinkKubeClient kubeClient, String configMapName) {
		this.kubeClient = checkNotNull(kubeClient, "Kubernetes client should not be null.");
		this.configMapName = checkNotNull(configMapName, "ConfigMap name should not be null.");
		this.configMapNotExistSupplier =
			() -> new FlinkRuntimeException("ConfigMap " + configMapName + " does not exist.");

		this.running = false;
	}

	@Override
	public void start() {
		synchronized (lock) {
			if (!running) {
				running = true;
			}
		}
	}

	@Override
	public void shutdown(JobStatus jobStatus) {
		synchronized (lock) {
			if (!running) {
				return;
			}
			LOG.info("Shutting down.");
			if (jobStatus.isGloballyTerminalState()) {
				LOG.info("Removing ConfigMap {}", configMapName);
				kubeClient.deleteConfigMap(configMapName);
			}
			running = false;
		}
	}

	@Override
	public long getAndIncrement() throws Exception {
		final AtomicLong newCounter = new AtomicLong();
		kubeClient.checkAndUpdateConfigMap(
			configMapName,
			KubernetesUtils.getLeaderChecker(),
			configMap -> {
				final long newCount = getCurrentCounter(configMap) + 1;
				configMap.getData().put(CHECKPOINT_COUNTER_KEY, String.valueOf(newCount));
				newCounter.set(newCount);
				return configMap;
			}
		).get();
		return newCounter.get();
	}

	@Override
	public long get() {
		return kubeClient.getConfigMap(configMapName)
			.map(this::getCurrentCounter)
			.orElseThrow(configMapNotExistSupplier);
	}

	@Override
	public void setCount(long newCount) throws Exception {
		kubeClient.checkAndUpdateConfigMap(
			configMapName,
			KubernetesUtils.getLeaderChecker(),
			configMap -> {
				configMap.getData().put(CHECKPOINT_COUNTER_KEY, String.valueOf(newCount));
				return configMap;
			}
		).get();
	}

	private long getCurrentCounter(KubernetesConfigMap configMap) {
		if (configMap.getData().containsKey(CHECKPOINT_COUNTER_KEY)) {
			return Long.valueOf(configMap.getData().get(CHECKPOINT_COUNTER_KEY));
		} else {
			return 1;
		}
	}
}
