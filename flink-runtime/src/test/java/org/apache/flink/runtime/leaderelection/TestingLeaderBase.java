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

import javax.annotation.Nullable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Base class which provides some convenience functions for testing purposes of {@link LeaderContender} and
 * {@link LeaderElectionEventHandler}.
 */
public class TestingLeaderBase {
	// The queues will be offered by subclasses
	protected final BlockingQueue<LeaderInformation> leaderQueue = new LinkedBlockingQueue<>();
	protected final BlockingQueue<LeaderInformation> revokeQueue = new LinkedBlockingQueue<>();
	protected final BlockingQueue<Throwable> errorQueue = new LinkedBlockingQueue<>();

	private boolean isLeader = false;
	private Throwable error;

	public void waitForLeader(long timeout) throws Exception {
		final LeaderInformation leader = leaderQueue.poll(timeout, TimeUnit.MILLISECONDS);
		if (leader == null || leader.equals(LeaderInformation.empty())) {
			throw new TimeoutException("Contender was not elected as the leader within " + timeout + "ms");
		}
		isLeader = true;
	}

	public void waitForRevokeLeader(long timeout) throws Exception {
		final LeaderInformation revoke = revokeQueue.poll(timeout, TimeUnit.MILLISECONDS);
		if (revoke == null || !revoke.equals(LeaderInformation.empty())) {
			throw new TimeoutException("Contender was not revoked within " + timeout + "ms");
		}
		isLeader = false;
	}

	public void waitForError(long timeout) throws Exception {
		final Throwable throwable = errorQueue.poll(timeout, TimeUnit.MILLISECONDS);
		if (throwable == null) {
			throw new TimeoutException("Contender did not see an exception with " + timeout + "ms");
		}
		error = throwable;
	}

	@Nullable
	public Throwable getError() {
		return error;
	}

	public boolean isLeader() {
		return isLeader;
	}
}
