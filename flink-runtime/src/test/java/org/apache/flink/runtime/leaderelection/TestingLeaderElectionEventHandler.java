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
import java.util.ArrayList;
import java.util.List;

/**
 * {@link LeaderElectionEventHandler} implementation which provides some convenience functions for testing
 * purposes. Please note that the {@link TestingLeaderElectionEventHandler} will not write leader information
 * to external storage.
 */
public class TestingLeaderElectionEventHandler extends TestingLeaderBase implements LeaderElectionEventHandler {

	private final List<LeaderInformation> leaderChanges = new ArrayList<>();

	private final LeaderInformation leaderInformation;

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
		if (leaderElectionDriver == null) {
			throw new FlinkRuntimeException("init() should be called first.");
		}
		confirmedLeaderInformation = leaderInformation;
		leaderElectionDriver.writeLeaderInformation(confirmedLeaderInformation);
		leaderQueue.offer(confirmedLeaderInformation);
	}

	@Override
	public void onRevokeLeadership() {
		confirmedLeaderInformation = LeaderInformation.empty();
		revokeQueue.offer(confirmedLeaderInformation);
	}

	@Override
	public void onLeaderInformationChange(LeaderInformation leaderInformation) {
		if (!this.leaderInformation.equals(leaderInformation)) {
			this.leaderChanges.add(leaderInformation);
		}
	}

	public void handleError(Throwable ex) {
		errorQueue.offer(ex);
	}

	public LeaderInformation getConfirmedLeaderInformation() {
		return confirmedLeaderInformation;
	}

	public void waitForLeaderChange(LeaderInformation leaderInformation, long timeout) throws Exception {
		CommonTestUtils.waitUntilCondition(
			() -> leaderChanges.contains(leaderInformation), Deadline.fromNow(Duration.ofMillis(timeout)));
	}
}
