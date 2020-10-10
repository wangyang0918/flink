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

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.state.ConnectionState;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;

/**
 * Leader election service for multiple JobManager. The leading JobManager is elected using
 * ZooKeeper. The current leader's address as well as its leader session ID is published via
 * ZooKeeper as well.
 */
public class ZooKeeperLeaderElectionService extends AbstractLeaderElectionService implements LeaderLatchListener, NodeCacheListener, UnhandledErrorListener {

	/** Client to the ZooKeeper quorum. */
	private final CuratorFramework client;

	/** Curator recipe for leader election. */
	private final LeaderLatch leaderLatch;

	/** Curator recipe to watch a given ZooKeeper node for changes. */
	private final NodeCache cache;

	/** ZooKeeper path of the node which stores the current leader information. */
	private final String leaderPath;

	private final ConnectionStateListener listener = new ConnectionStateListener() {
		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			handleStateChange(newState);
		}
	};

	/**
	 * Creates a ZooKeeperLeaderElectionService object.
	 *
	 * @param client Client which is connected to the ZooKeeper quorum
	 * @param latchPath ZooKeeper node path for the leader election latch
	 * @param leaderPath ZooKeeper node path for the node which stores the current leader information
	 */
	public ZooKeeperLeaderElectionService(CuratorFramework client, String latchPath, String leaderPath) {
		this.client = Preconditions.checkNotNull(client, "CuratorFramework client");
		this.leaderPath = Preconditions.checkNotNull(leaderPath, "leaderPath");

		leaderLatch = new LeaderLatch(client, latchPath);
		cache = new NodeCache(client, leaderPath);
	}

	@Override
	public void internalStart(LeaderContender contender) throws Exception {
		client.getUnhandledErrorListenable().addListener(this);

		leaderLatch.addListener(this);
		leaderLatch.start();

		cache.getListenable().addListener(this);
		cache.start();

		client.getConnectionStateListenable().addListener(listener);
	}

	@Override
	public void internalStop() throws Exception{
		client.getUnhandledErrorListenable().removeListener(this);

		client.getConnectionStateListenable().removeListener(listener);

		Exception exception = null;

		try {
			cache.close();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			leaderLatch.close();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			throw new Exception("Could not properly stop the ZooKeeperLeaderElectionService.", exception);
		}
	}

	@Override
	public boolean checkLeaderLatch() {
		return leaderLatch.hasLeadership();
	}

	@Override
	public void isLeader() {
		onGrantLeadership();
	}

	@Override
	public void notLeader() {
		onRevokeLeadership();
	}

	@Override
	public void nodeChanged() throws Exception {
		try {
			// leaderSessionID is null if the leader contender has not yet confirmed the session ID
			if (leaderLatch.hasLeadership()) {
				synchronized (lock) {
					if (running) {
						if (logger.isDebugEnabled()) {
							logger.debug(
								"Leader node changed while {} is the leader with session ID {}.",
								leaderContender.getDescription(),
								confirmedLeaderSessionID);
						}

						if (confirmedLeaderSessionID != null) {
							ChildData childData = cache.getCurrentData();

							if (childData == null) {
								if (logger.isDebugEnabled()) {
									logger.debug(
										"Writing leader information into empty node by {}.",
										leaderContender.getDescription());
								}
								writeLeaderInformation();
							} else {
								byte[] data = childData.getData();

								if (data == null || data.length == 0) {
									// the data field seems to be empty, rewrite information
									if (logger.isDebugEnabled()) {
										logger.debug(
											"Writing leader information into node with empty data field by {}.",
											leaderContender.getDescription());
									}
									writeLeaderInformation();
								} else {
									ByteArrayInputStream bais = new ByteArrayInputStream(data);
									ObjectInputStream ois = new ObjectInputStream(bais);

									String leaderAddress = ois.readUTF();
									UUID leaderSessionID = (UUID) ois.readObject();

									if (!leaderAddress.equals(confirmedLeaderAddress) ||
										(leaderSessionID == null || !leaderSessionID.equals(confirmedLeaderSessionID))) {
										// the data field does not correspond to the expected leader information
										if (logger.isDebugEnabled()) {
											logger.debug(
												"Correcting leader information by {}.",
												leaderContender.getDescription());
										}
										writeLeaderInformation();
									}
								}
							}
						}
					} else {
						logger.debug("Ignoring node change notification since the service has already been stopped.");
					}
				}
			}
		} catch (Exception e) {
			leaderContender.handleError(new Exception("Could not handle node changed event.", e));
			throw e;
		}
	}

	/**
	 * Writes the current leader's address as well the given leader session ID to ZooKeeper.
	 */
	@Override
	protected void writeLeaderInformation() {
		// this method does not have to be synchronized because the curator framework client
		// is thread-safe
		try {
			if (logger.isDebugEnabled()) {
				logger.debug(
					"Write leader information: Leader={}, session ID={}.",
					confirmedLeaderAddress,
					confirmedLeaderSessionID);
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			oos.writeUTF(confirmedLeaderAddress);
			oos.writeObject(confirmedLeaderSessionID);

			oos.close();

			boolean dataWritten = false;

			while (!dataWritten && leaderLatch.hasLeadership()) {
				Stat stat = client.checkExists().forPath(leaderPath);

				if (stat != null) {
					long owner = stat.getEphemeralOwner();
					long sessionID = client.getZookeeperClient().getZooKeeper().getSessionId();

					if (owner == sessionID) {
						try {
							client.setData().forPath(leaderPath, baos.toByteArray());

							dataWritten = true;
						} catch (KeeperException.NoNodeException noNode) {
							// node was deleted in the meantime
						}
					} else {
						try {
							client.delete().forPath(leaderPath);
						} catch (KeeperException.NoNodeException noNode) {
							// node was deleted in the meantime --> try again
						}
					}
				} else {
					try {
						client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(
								leaderPath,
								baos.toByteArray());

						dataWritten = true;
					} catch (KeeperException.NodeExistsException nodeExists) {
						// node has been created in the meantime --> try again
					}
				}
			}

			if (logger.isDebugEnabled()) {
				logger.debug(
					"Successfully wrote leader information: Leader={}, session ID={}.",
					confirmedLeaderAddress,
					confirmedLeaderSessionID);
			}
		} catch (Exception e) {
			leaderContender.handleError(
					new Exception("Could not write leader address and leader session ID to " +
							"ZooKeeper.", e));
		}
	}

	protected void handleStateChange(ConnectionState newState) {
		switch (newState) {
			case CONNECTED:
				logger.debug("Connected to ZooKeeper quorum. Leader election can start.");
				break;
			case SUSPENDED:
				logger.warn("Connection to ZooKeeper suspended. The contender " + leaderContender.getDescription()
					+ " no longer participates in the leader election.");
				break;
			case RECONNECTED:
				logger.info("Connection to ZooKeeper was reconnected. Leader election can be restarted.");
				break;
			case LOST:
				// Maybe we have to throw an exception here to terminate the JobManager
				logger.warn("Connection to ZooKeeper lost. The contender " + leaderContender.getDescription()
					+ " no longer participates in the leader election.");
				break;
		}
	}

	@Override
	public void unhandledError(String message, Throwable e) {
		leaderContender.handleError(new FlinkException("Unhandled error in ZooKeeperLeaderElectionService: " + message, e));
	}

	@Override
	public String toString() {
		return "ZooKeeperLeaderElectionService{" +
			"leaderPath='" + leaderPath + '\'' +
			'}';
	}
}
