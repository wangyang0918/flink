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

package org.apache.flink.kubernetes.itcases;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.DefaultKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.StringUtils;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Base class for kubernetes integration tests with a configured real Kubernetes cluster and client. All the
 * ITCases assume that the environment ITCASE_KUBECONFIG is set with a valid kube config file. In the E2E tests, we
 * will use a minikube for the testing.
 */
public class KubernetesITTestBase {

	protected static final long TIMEOUT = 120L * 1000L;

	private static final String CLUSTER_ID = "flink-itcase-cluster";

	protected final KubeClientFactory kubeClientFactory = new DefaultKubeClientFactory();

	private static String kubeConfigFile;
	protected Configuration configuration;
	protected FlinkKubeClient flinkKubeClient;
	protected ExecutorService executorService;

	@BeforeClass
	public static void checkEnv() {
		final String kubeConfigEnv = System.getenv("ITCASE_KUBECONFIG");
		Assume.assumeTrue("ITCASE_KUBECONFIG environment is not set.",
			!StringUtils.isNullOrWhitespaceOnly(kubeConfigEnv));
		kubeConfigFile = kubeConfigEnv;
	}

	@Before
	public void setup() throws Exception {
		configuration = new Configuration();
		configuration.set(KubernetesConfigOptions.KUBE_CONFIG_FILE, kubeConfigFile);
		configuration.setString(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
		executorService = Executors.newFixedThreadPool(8, new ExecutorThreadFactory("IO-Executor"));
		flinkKubeClient = kubeClientFactory.fromConfiguration(configuration, executorService);
	}

	@After
	public void teardown() throws Exception {
		flinkKubeClient.close();
		executorService.shutdownNow();
		executorService.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
	}
}
