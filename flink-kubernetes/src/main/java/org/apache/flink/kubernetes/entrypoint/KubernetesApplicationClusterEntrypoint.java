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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.kubernetes.executors.KubernetesApplicationExecutorServiceLoader;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Javadoc.
 */
@Internal
public class KubernetesApplicationClusterEntrypoint extends ClusterEntrypoint {

	public KubernetesApplicationClusterEntrypoint(final Configuration configuration) {
		super(configuration);
	}

	@Override
	protected DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return DefaultDispatcherResourceManagerComponentFactory
				.createApplicationComponentFactory(KubernetesResourceManagerFactory.getInstance());
	}

	@Override
	protected ArchivedExecutionGraphStore createSerializableExecutionGraphStore(Configuration configuration, ScheduledExecutor scheduledExecutor) {
		return new MemoryArchivedExecutionGraphStore(); // TODO: 30.01.20 think about the fault-tolerance in this case.
	}

	public static void main(String[] args) throws IOException, ProgramInvocationException {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, KubernetesApplicationClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		final Configuration configuration = KubernetesEntrypointUtils.loadConfiguration();

		final KubernetesApplicationClusterEntrypoint kubernetesApplicationClusterEntrypoint =
				new KubernetesApplicationClusterEntrypoint(configuration);

		// Use explicit executor
		CompletableFuture.runAsync(() -> executeProgram(configuration, kubernetesApplicationClusterEntrypoint));
		ClusterEntrypoint.runClusterEntrypoint(kubernetesApplicationClusterEntrypoint);
	}

	private static void executeProgram(Configuration config, KubernetesApplicationClusterEntrypoint entrypoint) {
		final PackagedProgram executable;
		try {
			executable = getExecutable(config);
		} catch (ProgramInvocationException e) {
			throw new FlinkRuntimeException(e);
		}

		final PipelineExecutorServiceLoader executorServiceLoader = new KubernetesApplicationExecutorServiceLoader(
			entrypoint.getDispatcherGatewayRetrieverFuture()
		);

		try {
			// TODO: 05.02.20 rename clientUtils to submitUtils or sth...
			ClientUtils.executeProgram(executorServiceLoader, config, executable);
		} catch (Exception e) {
			LOG.warn("Could not execute program: ", e);
		}
	}

	private static PackagedProgram getExecutable(final Configuration config) throws ProgramInvocationException {

		final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(config);

		final List<URL> jars = configAccessor.getJars();
		final List<URL> classpaths = configAccessor.getClasspaths();
		final String entryPointClass = configAccessor.getMainClassName();
		final String[] programArgs = configAccessor.getProgramArgs().toArray(new String[0]);
		final SavepointRestoreSettings savepointRestoreSettings = configAccessor.getSavepointRestoreSettings();

		final File jar = new File(jars.get(0).getPath());

		return PackagedProgram.newBuilder()
			.setJarFile(jar)
			.setUserClassPaths(classpaths)
			.setEntryPointClassName(entryPointClass)
			.setConfiguration(config)
			.setSavepointRestoreSettings(savepointRestoreSettings)
			.setArguments(programArgs)
			.build();
	}
}
