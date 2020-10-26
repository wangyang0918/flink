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
import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderInformation;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_ADDRESS_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_SESSION_ID_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link DefaultLeaderElectionService} with {@link KubernetesLeaderElectionDriver}.
 */
public class KubernetesLeaderElectionTest extends KubernetesHighAvailabilityTestBase {

	@Test
	public void testKubernetesLeaderElection() throws Exception {
		new Context() {{
			runTestAndGrantLeadershipToContender(
				() -> {
					assertThat(electionEventHandler.getLeaderInformation().getLeaderAddress(), is(LEADER_URL));

					// Revoke leader
					leaderController.set(false);
					electionEventHandler.waitForRevokeLeader(TIMEOUT);
					assertThat(electionEventHandler.getLeaderInformation(), is(LeaderInformation.empty()));
				});
		}};
	}

	@Test
	public void testLeaderConfigMapDeletedExternally() throws Exception {
		new Context() {{
			runTestAndGrantLeadershipToContender(
				() -> {
					assertThat(configMapCallbackFutures.size(), is(2));
					final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> callbackHandler =
						configMapCallbackFutures.get(0).get(TIMEOUT, TimeUnit.MILLISECONDS);
					assertThat(callbackHandler, is(notNullValue()));

					final KubernetesConfigMap configMap = getLeaderConfigMap();
					flinkKubeClient.deleteConfigMap(LEADER_CONFIGMAP_NAME).get();
					callbackHandler.onDeleted(Collections.singletonList(configMap));
					electionEventHandler.waitForError(TIMEOUT);
					final String errorMsg = "ConfigMap " + LEADER_CONFIGMAP_NAME + " is deleted externally";
					assertThat(electionEventHandler.getError(), is(notNullValue()));
					assertThat(electionEventHandler.getError().getMessage(), containsString(errorMsg));
				});
		}};
	}

	@Test
	public void testLeaderConfigMapUpdatedExternally() throws Exception {
		new Context() {{
			runTestAndGrantLeadershipToContender(
				() -> {
					assertThat(configMapCallbackFutures.size(), is(2));
					final FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> callbackHandler =
						configMapCallbackFutures.get(0).get(TIMEOUT, TimeUnit.MILLISECONDS);
					assertThat(callbackHandler, is(notNullValue()));
					// Update ConfigMap with wrong data
					final KubernetesConfigMap updatedConfigMap = getLeaderConfigMap();
					final LeaderInformation faultyLeader = LeaderInformation.known(
						UUID.randomUUID(), "faultyLeaderAddress");
					updatedConfigMap.getData().put(LEADER_ADDRESS_KEY, faultyLeader.getLeaderAddress());
					updatedConfigMap.getData().put(LEADER_SESSION_ID_KEY, faultyLeader.getLeaderSessionID().toString());
					callbackHandler.onModified(Collections.singletonList(updatedConfigMap));
					electionEventHandler.waitForLeaderChange(faultyLeader, TIMEOUT);
				});
		}};
	}

	@Test
	public void testHighAvailabilityLabelsCorrectlySet() throws Exception {
		new Context() {{
			runTestAndGrantLeadershipToContender(
				() -> {
					leaderElectionDriver.writeLeaderInformation(electionEventHandler.getLeaderInformation());
					final Map<String, String> leaderLabels = getLeaderConfigMap().getLabels();
					assertThat(leaderLabels.size(), is(3));
					assertThat(leaderLabels.get(LABEL_CONFIGMAP_TYPE_KEY), is(LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
				});
		}};
	}
}
