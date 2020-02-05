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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.YarnConfigKeys;
import org.apache.flink.yarn.entrypoint.application.Utils;
import org.apache.flink.yarn.executors.YarnApplicationExecutorServiceLoader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Javadoc.
 */
@Internal
public class YarnApplicationClusterEntrypoint extends ClusterEntrypoint {

	public YarnApplicationClusterEntrypoint(final Configuration configuration) {
		super(configuration);
	}

	@Override
	protected DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return DefaultDispatcherResourceManagerComponentFactory
				.createApplicationComponentFactory(YarnResourceManagerFactory.getInstance());
	}

	@Override
	protected ArchivedExecutionGraphStore createSerializableExecutionGraphStore(Configuration configuration, ScheduledExecutor scheduledExecutor) {
		return new MemoryArchivedExecutionGraphStore(); // TODO: 30.01.20 think about the fault-tolerance in this case.
	}

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, YarnApplicationClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		Map<String, String> env = System.getenv();

		final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
		Preconditions.checkArgument(
				workingDirectory != null,
				"Working directory variable (%s) not set",
				ApplicationConstants.Environment.PWD.key());

		try {
			YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
		} catch (IOException e) {
			LOG.warn("Could not log YARN environment information.", e);
		}

		final YarnConfiguration yarnConfiguration = new YarnConfiguration();
		final Configuration configuration = YarnEntrypointUtils.loadConfiguration(workingDirectory, env);

		final YarnApplicationClusterEntrypoint yarnApplicationClusterEntrypoint =
				new YarnApplicationClusterEntrypoint(configuration);

		ClusterEntrypoint.runClusterEntrypoint(yarnApplicationClusterEntrypoint);

		// TODO: 30.01.20 we will have to pass information to the executor, like the
		//  gateway through which to submit jobs to the cluster

		try {
			final List<File> localizedJars = getJars(yarnConfiguration, configuration, env); // TODO: 05.02.20 clean all this up
			final PackagedProgram executable = Utils.getPackagedProgram(configuration, localizedJars);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Launching application in Application Mode with config: {}", configuration);
			}

			final PipelineExecutorServiceLoader executorServiceLoader = new YarnApplicationExecutorServiceLoader(
					yarnApplicationClusterEntrypoint.getDispatcherGatewayRetrieverFuture()
			);

			// TODO: 05.02.20 rename clientUtils to submitUtils or sth...
			ClientUtils.executeProgram(executorServiceLoader, configuration, executable);
		} catch (Exception e) {
			LOG.warn("Could not execute program: ", e);
		}
	}

	public static List<File> getJars(
			final YarnConfiguration yarnConfig,
			final Configuration configuration,
			final Map<String, String> environment) throws IOException {

		checkNotNull(yarnConfig);
		checkNotNull(environment);

		final String userJars = environment.get(YarnConfigKeys.ENV_JAR_FILES).split(",")[0].split("=")[1];
		checkState(userJars != null, "Environment variable %s not set", YarnConfigKeys.ENV_JAR_FILES);

		// TODO: 05.02.20 NEW JIRA the relocator should become an interface where the local path at the execution is computed by us
		final FileSystem fs = FileSystem.get(yarnConfig);
		final Path srcPath = new Path(userJars);
		final Path dstPath = new Path(srcPath.getName());
		fs.copyToLocalFile(srcPath, dstPath);

		final URL p = new File(dstPath.toUri().getPath()).getAbsoluteFile().toURI().toURL();
		LOG.info("URL: {}", p.toString());

		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.JARS, Collections.singleton(p), URL::toString);
		return Collections.singletonList(new File(dstPath.toUri().getPath()));
	}
}
