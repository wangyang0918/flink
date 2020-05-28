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

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.utils.Constants;

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link KubernetesLeaderRetrievalService}.
 */
public class KubernetesLeaderRetrievalServiceTest extends KubernetesHighAvailabilityTestBase {

	@Test
	public void testKubernetesLeaderRetrievalOnAdded() throws Exception {
		new Context() {{
			runTest(
				() -> {
					final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> callbackHandler =
						leaderRetrievalConfigMapCallback.get(TIMEOUT, TimeUnit.MILLISECONDS);
					assertThat(callbackHandler, is(notNullValue()));
					// A new leader is elected.
					callbackHandler.onAdded(Collections.singletonList(configMapStore.get(LEADER_CONFIGMAP_NAME)));
					listener.waitForNewLeader(TIMEOUT);
					assertThat(listener.getAddress(), is(LEADER_URL));
				});
		}};
	}

	@Test
	public void testKubernetesLeaderRetrievalOnModified() throws Exception {
		new Context() {{
			runTest(
				() -> {
					final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> callbackHandler =
						leaderRetrievalConfigMapCallback.get(TIMEOUT, TimeUnit.MILLISECONDS);
					assertThat(callbackHandler, is(notNullValue()));
					// Leader changed
					final String newLeader = LEADER_URL + "_" + 2;
					configMapStore.get(LEADER_CONFIGMAP_NAME).getData().put(Constants.LEADER_ADDRESS_KEY, newLeader);
					callbackHandler.onModified(Collections.singletonList(configMapStore.get(LEADER_CONFIGMAP_NAME)));
					listener.waitForNewLeader(TIMEOUT);
					assertThat(listener.getAddress(), is(newLeader));
				});
		}};
	}

	@Test
	public void testLeaderConfigMapWronglyUpdated() throws Exception {
		new Context() {{
			runTest(
				() -> {
					final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> callbackHandler =
						leaderRetrievalConfigMapCallback.get(TIMEOUT, TimeUnit.MILLISECONDS);
					assertThat(callbackHandler, is(notNullValue()));
					// Leader wrongly set
					configMapStore.get(LEADER_CONFIGMAP_NAME).getData().clear();
					callbackHandler.onModified(Collections.singletonList(configMapStore.get(LEADER_CONFIGMAP_NAME)));
					assertThat(listener.getAddress(), is(nullValue()));
				});
		}};
	}

	@Test
	public void testKubernetesLeaderRetrievalOnDeleted() throws Exception {
		new Context() {{
			runTest(
				() -> {
					final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> callbackHandler =
						leaderRetrievalConfigMapCallback.get(TIMEOUT, TimeUnit.MILLISECONDS);
					assertThat(callbackHandler, is(notNullValue()));
					// Leader ConfigMap is deleted
					callbackHandler.onDeleted(Collections.singletonList(configMapStore.get(LEADER_CONFIGMAP_NAME)));
					assertThat(listener.getAddress(), is(nullValue()));
				});
		}};
	}
}
