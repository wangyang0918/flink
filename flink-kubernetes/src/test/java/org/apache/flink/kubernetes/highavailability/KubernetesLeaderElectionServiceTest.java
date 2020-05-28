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
import org.apache.flink.runtime.leaderelection.TestingContender;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_KEY;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link KubernetesLeaderElectionService}.
 */
public class KubernetesLeaderElectionServiceTest extends KubernetesHighAvailabilityTestBase {

	@Test
	public void testKubernetesLeaderElection() throws Exception {
		new Context() {{
			runTest(
				() -> {
					assertThat(leaderElectionService.getLeaderSessionID(), is(contender.getLeaderSessionID()));
					assertThat(configMapStore.size(), is(1));
					assertThat(getLeaderConfigMap().getData().get(Constants.LEADER_ADDRESS_KEY), is(LEADER_URL));

					// Revoke leader
					leaderController.set(false);
					contender.waitForRevokeLeader(TIMEOUT);
					assertThat(leaderElectionService.getLeaderSessionID(), nullValue());
					assertThat(getLeaderConfigMap().getData().size(), is(0));
				});
		}};
	}

	@Test
	public void testLeaderConfigMapDeletedExternally() throws Exception {
		new Context() {{
			runTest(
				() -> {
					final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> callbackHandler =
						configMapsAndDoCallbackFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
					assertThat(callbackHandler, is(notNullValue()));

					callbackHandler.onDeleted(Collections.singletonList(configMapStore.remove(LEADER_CONFIGMAP_NAME)));
					// The ConfigMap should be created again.
					assertThat(getLeaderConfigMap().getData().get(Constants.LEADER_ADDRESS_KEY), is(LEADER_URL));
				});
		}};
	}

	@Test
	public void testLeaderConfigMapUpdatedExternally() throws Exception {
		new Context() {{
			runTest(
				() -> {
					final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> callbackHandler =
						configMapsAndDoCallbackFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
					assertThat(callbackHandler, is(notNullValue()));
					// Update ConfigMap with wrong data
					getLeaderConfigMap().getData().put(Constants.LEADER_ADDRESS_KEY, "wrong data");
					callbackHandler.onModified(Collections.singletonList(configMapStore.get(LEADER_CONFIGMAP_NAME)));
					// The ConfigMap should be corrected
					assertThat(getLeaderConfigMap().getData().get(Constants.LEADER_ADDRESS_KEY), is(LEADER_URL));
				});
		}};
	}

	/**
	 * Start multiple leaders, if current leader dies, a new one could take over and update the leader
	 * information successfully.
	 */
	@Test
	public void testMultipleLeaders() throws Exception {
		final int leaderNum = 3;
		final AtomicBoolean[] leaderController = new AtomicBoolean[leaderNum];
		final TestingContender[] contenders = new TestingContender[leaderNum];
		final KubernetesLeaderElectionService[] leaderElectionServices = new KubernetesLeaderElectionService[leaderNum];
		for (int i = 0; i < leaderNum; i++) {
			leaderController[i] = new AtomicBoolean(false);
			leaderElectionServices[i] = createLeaderElectionService(leaderController[i]);
			contenders[i] = new TestingContender(getLeaderUrl(i), leaderElectionServices[i]);
			leaderElectionServices[i].start(contenders[i]);
		}
		leaderController[0].set(true);
		contenders[0].waitForLeader(TIMEOUT);
		assertThat(
			configMapStore.get(LEADER_CONFIGMAP_NAME).getData().get(Constants.LEADER_ADDRESS_KEY),
			is(getLeaderUrl(0)));
		// Leader 0 died
		leaderController[0].set(false);
		contenders[0].waitForRevokeLeader(TIMEOUT);
		// Leader 2 try to acquire
		leaderController[2].set(true);
		contenders[2].waitForLeader(TIMEOUT);
		assertThat(
			configMapStore.get(LEADER_CONFIGMAP_NAME).getData().get(Constants.LEADER_ADDRESS_KEY),
			is(getLeaderUrl(2)));
		for (int i = 0; i < leaderNum; i++) {
			leaderElectionServices[i].stop();
		}
	}

	@Test
	public void testHighAvailabilityLabelsCorrectlySet() throws Exception {
		new Context() {{
			runTest(
				() -> {
					final Map<String, String> leaderLabels = getLeaderConfigMap().getLabels();
					assertThat(leaderLabels.size(), is(3));
					assertThat(leaderLabels.get(LABEL_CONFIGMAP_TYPE_KEY), is(LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
				});
		}};
	}

	private String getLeaderUrl(int i) {
		return LEADER_URL + "_" + i;
	}
}
