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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.time.Duration;

/**
 * {@link LeaderElectionEventHandler} implementation which provides some convenience functions for testing
 * purposes.
 */
public class TestingLeaderElectionEventHandler extends TestingLeaderBase implements LeaderElectionEventHandler {

	private final LeaderInformation leaderInformation;

	private final Duration timeout = Duration.ofMillis(120 * 1000);

	private LeaderInformation confirmedLeaderInformation = LeaderInformation.empty();

	private LeaderElectionDriver leaderElectionDriver = null;

	public TestingLeaderElectionEventHandler(LeaderInformation leaderInformation) {
		this.leaderInformation = leaderInformation;
	}

	public void init(LeaderElectionDriver leaderElectionDriver) {
		this.leaderElectionDriver = leaderElectionDriver;
	}

	@Override
	public void onGrantLeadership() {
		waitForInit();
		confirmedLeaderInformation = leaderInformation;
		leaderElectionDriver.writeLeaderInformation(confirmedLeaderInformation);
		leaderEventQueue.offer(confirmedLeaderInformation);
	}

	@Override
	public void onRevokeLeadership() {
		waitForInit();
		confirmedLeaderInformation = LeaderInformation.empty();
		leaderElectionDriver.writeLeaderInformation(confirmedLeaderInformation);
		leaderEventQueue.offer(confirmedLeaderInformation);
	}

	@Override
	public void onLeaderInformationChange(LeaderInformation leaderInformation) {
		waitForInit();
		if (confirmedLeaderInformation.getLeaderSessionID() != null &&
			!this.confirmedLeaderInformation.equals(leaderInformation)) {
			leaderElectionDriver.writeLeaderInformation(confirmedLeaderInformation);
		}
	}

	public LeaderInformation getConfirmedLeaderInformation() {
		return confirmedLeaderInformation;
	}

	private void waitForInit() {
		try {
			CommonTestUtils.waitUntilCondition(() -> leaderElectionDriver != null, Deadline.fromNow(timeout));
		} catch (Exception ex) {
			throw new FlinkRuntimeException("init() was not called in " + timeout + ". It usually means that " +
				"creating LeaderElectionDriver takes too long or forgetting to call the init() method.");
		}
	}
}
