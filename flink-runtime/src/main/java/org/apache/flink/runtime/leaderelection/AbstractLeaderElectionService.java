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

import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Abstract class for leader election service based on distributed coordination system(e.g. Zookeeper, ETCD, etc.).
 */
public abstract class AbstractLeaderElectionService implements LeaderElectionService {

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final Object lock = new Object();

	/** The leader contender which applies for leadership. */
	protected volatile LeaderContender leaderContender;

	protected volatile UUID issuedLeaderSessionID;

	protected volatile UUID confirmedLeaderSessionID;

	protected volatile String confirmedLeaderAddress;

	protected volatile boolean running;

	protected AbstractLeaderElectionService() {
		leaderContender = null;

		issuedLeaderSessionID = null;
		confirmedLeaderSessionID = null;
		confirmedLeaderAddress = null;

		running = false;
	}

	@Override
	public final void start(LeaderContender contender) throws Exception {
		Preconditions.checkNotNull(contender, "Contender must not be null.");
		Preconditions.checkState(leaderContender == null, "Contender was already set.");

		logger.info("Starting LeaderElectionService {}.", this);

		synchronized (lock) {
			leaderContender = contender;
			running = true;
			internalStart(contender);
		}
	}

	@Override
	public final void stop() throws Exception {
		synchronized (lock) {
			if (!running) {
				return;
			}
			running = false;
			clearConfirmedLeaderInformation();
		}

		logger.info("Stopping LeaderElectionService {}.", this);

		internalStop();
	}

	@Override
	public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
		if (logger.isDebugEnabled()) {
			logger.debug(
				"Confirm leader session ID {} for leader {}.",
				leaderSessionID,
				leaderAddress);
		}

		Preconditions.checkNotNull(leaderSessionID);

		if (checkLeaderLatch()) {
			// check if this is an old confirmation call
			synchronized (lock) {
				if (running) {
					if (leaderSessionID.equals(this.issuedLeaderSessionID)) {
						confirmLeaderInformation(leaderSessionID, leaderAddress);
						writeLeaderInformation();
					}
				} else {
					logger.debug("Ignoring the leader session Id {} confirmation, since the " +
						"ZooKeeperLeaderElectionService has already been stopped.", leaderSessionID);
				}
			}
		} else {
			logger.warn("The leader session ID {} was confirmed even though the " +
				"corresponding JobManager was not elected as the leader.", leaderSessionID);
		}
	}

	/**
	 * Returns the current leader session ID or null, if the contender is not the leader.
	 *
	 * @return The last leader session ID or null, if the contender is not the leader
	 */
	public UUID getLeaderSessionID() {
		return confirmedLeaderSessionID;
	}

	protected abstract void internalStart(LeaderContender contender) throws Exception;

	protected abstract void internalStop() throws Exception;

	protected abstract void writeLeaderInformation();

	protected abstract boolean checkLeaderLatch();

	protected final void onGrantLeadership() {
		synchronized (lock) {
			if (running) {
				issuedLeaderSessionID = UUID.randomUUID();
				clearConfirmedLeaderInformation();

				if (logger.isDebugEnabled()) {
					logger.debug(
						"Grant leadership to contender {} with session ID {}.",
						leaderContender.getDescription(),
						issuedLeaderSessionID);
				}

				leaderContender.grantLeadership(issuedLeaderSessionID);
			} else {
				logger.debug("Ignoring the grant leadership notification since the service has " +
					"already been stopped.");
			}
		}
	}

	protected final void onRevokeLeadership() {
		synchronized (lock) {
			if (running) {
				logger.debug(
					"Revoke leadership of {} ({}@{}).",
					leaderContender.getDescription(),
					confirmedLeaderSessionID,
					confirmedLeaderAddress);

				issuedLeaderSessionID = null;
				clearConfirmedLeaderInformation();

				leaderContender.revokeLeadership();
			} else {
				logger.debug("Ignoring the revoke leadership notification since the service " +
					"has already been stopped.");
			}
		}
	}

	private void confirmLeaderInformation(UUID leaderSessionID, String leaderAddress) {
		confirmedLeaderSessionID = leaderSessionID;
		confirmedLeaderAddress = leaderAddress;
	}

	private void clearConfirmedLeaderInformation() {
		confirmedLeaderSessionID = null;
		confirmedLeaderAddress = null;
	}
}
