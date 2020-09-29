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
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Kubernetes based registry for running jobs, highly available. All the jobs running in a same Flink cluster will
 * share a ConfigMap to store the job statuses. The key is the job id, and value is job status.
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

		return kubeClient.getConfigMap(configMapName).map(
			configMap -> {
				final String status = configMap.getData().get(jobID.toString());
				if (!StringUtils.isNullOrWhitespaceOnly(status)) {
					return JobSchedulingStatus.valueOf(status);
				}
				return JobSchedulingStatus.PENDING;
			}).orElse(JobSchedulingStatus.PENDING);
	}

	@Override
	public void clearJob(JobID jobID) throws IOException {
		checkNotNull(jobID);

		final Optional<KubernetesConfigMap> optional = kubeClient.getConfigMap(configMapName);
		if (optional.isPresent()) {
			final KubernetesConfigMap configMap = optional.get();
			if (configMap.getData() != null && configMap.getData().remove(jobID.toString()) != null) {
				try {
					kubeClient.updateConfigMap(configMap).get();
				} catch (Exception e) {
					throw new IOException("Failed to clear job state from ConfigMap for job " + jobID, e);
				}
			}
		}
	}

	private void writeJobStatusToConfigMap(JobID jobID, JobSchedulingStatus status) throws IOException {
		LOG.debug("Setting scheduling state for job {} to {}.", jobID, status);
		final Optional<KubernetesConfigMap> optional = kubeClient.getConfigMap(configMapName);
		final Map<String, String> data = new HashMap<>();
		if (optional.isPresent()) {
			final KubernetesConfigMap configMap = optional.get();
			if (configMap.getData() != null) {
				data.putAll(configMap.getData());
			}
			data.put(jobID.toString(), status.name());
			configMap.setData(data);
			try {
				kubeClient.updateConfigMap(configMap).get();
			} catch (Exception e) {
				throw new IOException("Failed to set " + status.name() + " state in ConfigMap for job " + jobID, e);
			}
		} else {
			data.put(jobID.toString(), status.name());
			kubeClient.createConfigMap(configMapName, data, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY);
		}
	}
}

