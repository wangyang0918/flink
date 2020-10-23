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

import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Base class which provides some convenience functions for testing purposes of {@link LeaderRetrievalListener} and
 * {@link org.apache.flink.runtime.leaderretrieval.LeaderRetrievalEventHandler}.
 */
public class TestingRetrievalBase {

	private final BlockingQueue<LeaderInformation> leaderQueue = new LinkedBlockingQueue<>();

	private String address;
	private String oldAddress;
	private UUID leaderSessionID;
	private Exception exception;

	public String waitForNewLeader(long timeout) throws Exception {
		final LeaderInformation leader = leaderQueue.poll(timeout, TimeUnit.MILLISECONDS);

		if (exception != null) {
			throw exception;
		} else if (leader == null || leader.getLeaderAddress() == null ||
			leader.getLeaderAddress().equals(oldAddress)) {
			throw new TimeoutException("Listener was not notified about a new leader within " + timeout + "ms");
		}

		address = leader.getLeaderAddress();
		leaderSessionID = leader.getLeaderSessionID();
		oldAddress = leader.getLeaderAddress();

		return address;
	}

	public void handleError(Throwable throwable) {
		this.exception = new Exception(throwable);
	}

	public String getAddress() {
		return address;
	}

	public UUID getLeaderSessionID() {
		return leaderSessionID;
	}

	public void offerToLeaderQueue(LeaderInformation leaderInformation) {
		leaderQueue.offer(leaderInformation);
		this.leaderSessionID = leaderInformation.getLeaderSessionID();
		this.address = leaderInformation.getLeaderAddress();
	}

	public Exception getError() {
		return this.exception;
	}
}
