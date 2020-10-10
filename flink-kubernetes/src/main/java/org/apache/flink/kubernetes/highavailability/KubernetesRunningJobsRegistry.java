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

import org.apache.flink.api.common.JobID;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.kubernetes.utils.Constants.NAME_SEPARATOR;
import static org.apache.flink.kubernetes.utils.Constants.RUNNING_JOBS_REGISTRY_KEY_PREFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Kubernetes based registry for running jobs, highly available. All the running jobs will be stored in Dispatcher
 * leader ConfigMap. The key is the job id, and value is job status.
 */
public class KubernetesRunningJobsRegistry implements RunningJobsRegistry {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesRunningJobsRegistry.class);

	private final FlinkKubeClient kubeClient;

	private final String configMapName;

	public KubernetesRunningJobsRegistry(FlinkKubeClient kubeClient, String configMapName) {
		this.kubeClient = kubeClient;
		this.configMapName = configMapName;
	}

	@Override
	public void setJobRunning(JobID jobID) throws IOException {
		checkNotNull(jobID);

		writeJobStatusToConfigMap(jobID, JobSchedulingStatus.RUNNING);
	}

	@Override
	public void setJobFinished(JobID jobID) throws IOException {
		checkNotNull(jobID);

		writeJobStatusToConfigMap(jobID, JobSchedulingStatus.DONE);
	}

	@Override
	public JobSchedulingStatus getJobSchedulingStatus(JobID jobID) {
		checkNotNull(jobID);

		return kubeClient.getConfigMap(configMapName)
			.map(configMap -> {
				final String status = configMap.getData().get(getKeyForJobId(jobID));
				if (!StringUtils.isNullOrWhitespaceOnly(status)) {
					return JobSchedulingStatus.valueOf(status);
				}
				return JobSchedulingStatus.PENDING;
			})
			.orElse(JobSchedulingStatus.PENDING);
	}

	@Override
	public void clearJob(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			kubeClient.checkAndUpdateConfigMap(
				configMapName,
				KubernetesUtils.getLeaderChecker(),
				configMap -> {
					configMap.getData().remove(getKeyForJobId(jobID));
					return configMap;
				}
			).get();
		} catch (Exception e) {
			throw new IOException("Failed to clear job state from ConfigMap for job " + jobID, e);
		}
	}

	private void writeJobStatusToConfigMap(JobID jobID, JobSchedulingStatus status) throws IOException {
		LOG.debug("Setting scheduling state for job {} to {}.", jobID, status);
		final String key = getKeyForJobId(jobID);
		try {
			kubeClient.checkAndUpdateConfigMap(
				configMapName,
				KubernetesUtils.getLeaderChecker(),
				configMap -> {
					configMap.getData().put(key, status.name());
					return configMap;
				}
			).get();
		} catch (Exception e) {
			throw new IOException("Failed to set " + status.name() + " state in ConfigMap for job " + jobID, e);
		}
	}

	private String getKeyForJobId(JobID jobId) {
		return RUNNING_JOBS_REGISTRY_KEY_PREFIX + NAME_SEPARATOR + jobId.toString();
	}
}

