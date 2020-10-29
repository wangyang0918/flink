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

package org.apache.flink.kubernetes.kubeclient.resources;

import java.util.concurrent.BlockingQueue;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Testing implementation for {@link KubernetesLeaderElector.LeaderCallbackHandler}.
 */
public class TestingLeaderCallbackHandler extends KubernetesLeaderElector.LeaderCallbackHandler {

	private final BlockingQueue<String> leaderStore;
	private final String lockIdentity;
	private boolean isLeader;

	public TestingLeaderCallbackHandler(BlockingQueue<String> leaderStore, String lockIdentity) {
		this.leaderStore = leaderStore;
		this.lockIdentity = lockIdentity;
	}

	@Override
	public void isLeader() {
		isLeader = true;
		leaderStore.poll();
		leaderStore.offer(lockIdentity);
		assertThat(leaderStore.size(), is(1));
	}

	@Override
	public void notLeader() {
		isLeader = false;
	}

	public String getLockIdentity() {
		return lockIdentity;
	}

	public boolean hasLeadership() {
		return isLeader;
	}
}
