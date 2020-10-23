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

import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link DefaultLeaderElectionService}.
 */
public class DefaultLeaderElectionServiceTest extends TestLogger {

	private static final String TEST_URL = "akka//user/jobmanager";
	private static final long timeout = 30L * 1000L;

	@Test
	public void testOnGrantAndRevokeLeadership() throws Exception {
		new Context() {{
			runTest(() -> {
				// grant leadership
				testingLeaderElectionDriver.isLeader();

				testingContender.waitForLeader(timeout);
				assertThat(testingContender.getDescription(), is(TEST_URL));
				assertThat(testingContender.getLeaderSessionID(), is(leaderElectionService.getLeaderSessionID()));
				// Check the external storage
				assertThat(
					testingLeaderElectionDriver.getLeaderInformation(),
					is(LeaderInformation.known(leaderElectionService.getLeaderSessionID(), TEST_URL)));

				// revoke leadership
				testingLeaderElectionDriver.notLeader();
				testingContender.waitForRevokeLeader(timeout);
				assertThat(testingContender.getLeaderSessionID(), is(nullValue()));
				// External storage should be cleared
				assertThat(testingLeaderElectionDriver.getLeaderInformation(), is(LeaderInformation.empty()));
			});
		}};
	}

	@Test
	public void testLeaderInformationChangedAndShouldBeCorrected() throws Exception {
		new Context() {{
			runTest(() -> {
				testingLeaderElectionDriver.isLeader();

				// Leader information changed on external storage. It should be corrected.
				testingLeaderElectionDriver.leaderInformationChanged(LeaderInformation.empty());
				assertThat(testingLeaderElectionDriver.getLeaderInformation().getLeaderAddress(), is(TEST_URL));
				assertThat(testingLeaderElectionDriver.getLeaderInformation().getLeaderSessionID(),
					is(leaderElectionService.getLeaderSessionID()));

				testingLeaderElectionDriver.leaderInformationChanged(
					LeaderInformation.known(UUID.randomUUID(), "faulty-address"));
				assertThat(testingLeaderElectionDriver.getLeaderInformation().getLeaderAddress(), is(TEST_URL));
				assertThat(testingLeaderElectionDriver.getLeaderInformation().getLeaderSessionID(),
					is(leaderElectionService.getLeaderSessionID()));
			});
		}};
	}

	@Test
	public void testHasLeadership() throws Exception {
		new Context() {{
			runTest(() -> {
				testingLeaderElectionDriver.isLeader();
				final UUID currentLeaderSessionId = leaderElectionService.getLeaderSessionID();
				assertThat(currentLeaderSessionId, is(notNullValue()));
				assertThat(leaderElectionService.hasLeadership(currentLeaderSessionId), is(true));
				assertThat(leaderElectionService.hasLeadership(UUID.randomUUID()), is(false));

				leaderElectionService.stop();
				assertThat(leaderElectionService.hasLeadership(currentLeaderSessionId), is(false));
			});
		}};
	}

	@Test
	public void testLeaderInformationChangedWhenNoConfirmedSessionID() throws Exception {
		new Context() {{
			runTest(() -> {
				final LeaderInformation faultyLeader = LeaderInformation.known(UUID.randomUUID(), "faulty-address");
				testingLeaderElectionDriver.leaderInformationChanged(faultyLeader);
				// External storage should keep the wrong value.
				assertThat(testingLeaderElectionDriver.getLeaderInformation(), is(faultyLeader));
			});
		}};
	}

	@Test
	public void testOnGrantLeadershipHappenAfterStop() throws Exception {
		new Context() {{
			runTest(() -> {
				leaderElectionService.stop();
				testingLeaderElectionDriver.isLeader();
				// leader contender is not granted leadership
				assertThat(testingContender.getLeaderSessionID(), is(nullValue()));
			});
		}};
	}

	@Test
	public void testOnRevokeLeadershipHappenAfterStop() throws Exception {
		new Context() {{
			runTest(() -> {
				testingLeaderElectionDriver.isLeader();

				leaderElectionService.stop();
				testingLeaderElectionDriver.leaderInformationChanged(LeaderInformation.empty());

				// External storage should not be corrected
				assertThat(testingLeaderElectionDriver.getLeaderInformation(), is(LeaderInformation.empty()));
			});
		}};
	}

	@Test
	public void testOnLeaderInformationChangeHappenAfterStop() throws Exception {
		new Context() {{
			runTest(() -> {
				testingLeaderElectionDriver.isLeader();
				final UUID oldSessionId = leaderElectionService.getLeaderSessionID();
				assertThat(testingContender.getLeaderSessionID(), is(oldSessionId));

				leaderElectionService.stop();

				testingLeaderElectionDriver.notLeader();
				// leader contender is not revoked leadership
				assertThat(testingContender.getLeaderSessionID(), is(oldSessionId));
			});
		}};
	}

	@Test
	public void testOldConfirmLeaderInformation() throws Exception {
		new Context() {{
			runTest(() -> {
				testingLeaderElectionDriver.isLeader();
				final UUID currentLeaderSessionId = leaderElectionService.getLeaderSessionID();
				assertThat(currentLeaderSessionId, is(notNullValue()));

				// Old confirm call should not be issued.
				leaderElectionService.confirmLeadership(UUID.randomUUID(), TEST_URL);
				assertThat(leaderElectionService.getLeaderSessionID(), is(currentLeaderSessionId));
			});
		}};
	}

	private class Context {
		final TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory testingLeaderElectionDriverFactory =
			new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory();
		final DefaultLeaderElectionService leaderElectionService = new DefaultLeaderElectionService(
			testingLeaderElectionDriverFactory);
		final TestingContender testingContender = new TestingContender(TEST_URL, leaderElectionService);

		TestingLeaderElectionDriver testingLeaderElectionDriver;

		void runTest(RunnableWithException testMethod) throws Exception {
			leaderElectionService.start(testingContender);

			testingLeaderElectionDriver = testingLeaderElectionDriverFactory.getCurrentLeaderDriver();
			assertThat(testingLeaderElectionDriver, is(notNullValue()));
			testMethod.run();

			leaderElectionService.stop();
		}
	}
}
