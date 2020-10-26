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

package org.apache.flink.kubernetes.itcases;

import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.runtime.concurrent.FutureUtils;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * IT Tests for {@link org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient} with real K8s server and client.
 */
public class Fabric8FlinkKubeClientITCase extends KubernetesITTestBase {

	private static final String TEST_CONFIG_MAP_NAME = "test-config-map";

	private static final Map<String, String> data = new HashMap<String, String>() {
		{
			put("key1", "0");
			put("key2", "0");
			put("key3", "0");
		}
	};

	@Before
	public void setup() throws Exception {
		super.setup();
		flinkKubeClient.createConfigMap(new KubernetesConfigMap(
			new ConfigMapBuilder()
				.withNewMetadata()
				.withName(TEST_CONFIG_MAP_NAME)
				.endMetadata()
				.withData(data)
				.build())).get();
	}

	@After
	public void teardown() throws Exception {
		flinkKubeClient.deleteConfigMap(TEST_CONFIG_MAP_NAME).get();
		super.teardown();
	}

	/**
	 * {@link org.apache.flink.kubernetes.kubeclient.FlinkKubeClient#checkAndUpdateConfigMap} is a transactional
	 * operation, we should definitely guarantee that the concurrent modification could work.
	 */
	@Test
	public void testCheckAndUpdateConfigMapConcurrently() throws Exception {
		// Start multiple instances to update ConfigMap concurrently
		final List<CompletableFuture<Void>> futures = new ArrayList<>();
		final int target = 10;
		final int updateIntervalMs = 500;
		data.keySet().forEach(
			key -> futures.add(CompletableFuture.runAsync(() -> {
				for (int index = 0; index < target; index++) {
					try {
						final boolean updated = flinkKubeClient.checkAndUpdateConfigMap(
							TEST_CONFIG_MAP_NAME,
							configMap -> {
								final int newValue = Integer.valueOf(configMap.getData().get(key)) + 1;
								configMap.getData().put(key, String.valueOf(newValue));
								return Optional.of(configMap);
							}).get();
						assertThat(updated, is(true));
					} catch (Exception e) {
						Assert.fail("Should not throw exception." + e.getMessage());
					}
					try {
						// Simulate the update interval
						Thread.sleep(updateIntervalMs);
					} catch (InterruptedException e) {
						// noop
					}
				}
			}, executorService)));
		FutureUtils.waitForAll(futures).get(TIMEOUT, TimeUnit.MILLISECONDS);
		// All the value should be increased exactly to the target
		final Optional<KubernetesConfigMap> configMapOpt = flinkKubeClient.getConfigMap(TEST_CONFIG_MAP_NAME);
		assertThat(configMapOpt.isPresent(), is(true));
		assertThat(configMapOpt.get().getData().values(), everyItem(is(String.valueOf(target))));
	}
}
