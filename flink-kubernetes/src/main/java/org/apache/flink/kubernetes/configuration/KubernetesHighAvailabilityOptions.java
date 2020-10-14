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

package org.apache.flink.kubernetes.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to Kubernetes high-availability settings.
 * All the HA information relevant for a specific component will be stored in a single ConfigMap.
 * For example, the Dispatcher's ConfigMap would then contain the current leader, the running jobs
 * and the pointers to the persisted JobGraphs.
 * The JobManager's ConfigMap would then contain the current leader, the pointers to the checkpoints
 * and the checkpoint ID counter.
 *
 * <p>The ConfigMap name will be created with the following pattern.
 * e.g. k8s-ha-app1-restserver-leader, k8s-ha-app1-00000000000000000000000000000000-jobmanager-leader
 */
@PublicEvolving
public class KubernetesHighAvailabilityOptions {

	@Documentation.Section(Documentation.Sections.EXPERT_KUBERNETES_HIGH_AVAILABILITY)
	public static final ConfigOption<String> HA_KUBERNETES_LEADER_SUFFIX =
			key("high-availability.kubernetes.leader.suffix")
			.stringType()
			.defaultValue("leader")
			.withDescription("The ConfigMap suffix of the leader which contains the URL to the leader and the " +
				"current leader session ID. Leader elector will use the same ConfigMap for contending the lock.");

	@Documentation.Section(Documentation.Sections.EXPERT_KUBERNETES_HIGH_AVAILABILITY)
	public static final ConfigOption<Duration> KUBERNETES_LEASE_DURATION =
			key("high-availability.kubernetes.client.lease-duration")
			.durationType()
			.defaultValue(Duration.ofSeconds(30))
			.withDescription("Define the lease duration for the Kubernetes leader election in ms. The leader will " +
				"continuously renew its lease time to indicate its existence. And the followers will do a lease " +
				"checking against the current time. \"renewTime + leaseDuration > now\" means the leader is alive.");

	@Documentation.Section(Documentation.Sections.EXPERT_KUBERNETES_HIGH_AVAILABILITY)
	public static final ConfigOption<Duration> KUBERNETES_RENEW_DEADLINE =
			key("high-availability.kubernetes.client.renew-deadline")
			.durationType()
			.defaultValue(Duration.ofSeconds(15))
			.withDescription("Defines the deadline when the leader tries to renew the lease in ms. If it could not " +
				"succeed in the given time, the renew operation will be aborted.");

	@Documentation.Section(Documentation.Sections.EXPERT_KUBERNETES_HIGH_AVAILABILITY)
	public static final ConfigOption<Duration> KUBERNETES_RETRY_PERIOD =
			key("high-availability.kubernetes.client.retry-period")
			.durationType()
			.defaultValue(Duration.ofSeconds(3))
			.withDescription("Defines the pause between consecutive retries in ms. Both the leader and followers use " +
				"this value for the retry.");

	@Documentation.Section(Documentation.Sections.EXPERT_KUBERNETES_HIGH_AVAILABILITY)
	public static final ConfigOption<Integer> KUBERNETES_MAX_RETRY_ATTEMPTS =
		key("high-availability.kubernetes.client.max-retry-attempts")
			.intType()
			.defaultValue(5)
			.withDescription("Defines the number of retries before the client gives up. For example, updating the " +
				"ConfigMap.");

	/** Not intended to be instantiated. */
	private KubernetesHighAvailabilityOptions() {}
}
