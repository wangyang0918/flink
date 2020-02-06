/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn.entrypoint.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.yarn.YarnConfigKeys;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Javadoc.
 */
@Internal
public class ProgramUtils {

	public static PackagedProgram getExecutable(
			final YarnConfiguration yarnConfig,
			final Configuration config,
			final Map<String, String> environment) throws IOException, ProgramInvocationException {

		final Configuration effectiveConfig = getEffectiveConfiguration(yarnConfig, config, environment);
		final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(effectiveConfig);

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
				.setConfiguration(effectiveConfig)
				.setSavepointRestoreSettings(savepointRestoreSettings)
				.setArguments(programArgs)
				.build();
	}

	private static Configuration getEffectiveConfiguration(
			final YarnConfiguration yarnConfig,
			final Configuration configuration,
			final Map<String, String> environment) throws IOException {

		final FileSystem fileSystem = FileSystem.get(yarnConfig);
		final Path tmpDirToUse = new Path(ConfigurationUtils.parseTempDirectories(configuration)[0]);

		final List<Path> remotePathsToLocalize = jobGraphJarPathsToLocalize(environment);
		final List<URL> localJarPaths = localizeJobGraphJars(fileSystem, remotePathsToLocalize, tmpDirToUse);

		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.JARS, localJarPaths, URL::toString);
		return configuration;
	}

	private static List<URL> localizeJobGraphJars(
			final FileSystem fileSystem,
			final List<Path> remotePathsToLocalize,
			final Path localDestDir) throws IOException {

		checkNotNull(fileSystem);
		checkNotNull(remotePathsToLocalize);

		// TODO: 05.02.20 NEW JIRA the relocator should become
		//  an interface where the local path at the execution is computed by us

		final List<URL> localJarURLs = new ArrayList<>();
		for (Path srcPath : remotePathsToLocalize) {
			final Path dstPath = new Path(localDestDir, srcPath.getName());
			fileSystem.copyToLocalFile(srcPath, dstPath);

			final URL url = new File(dstPath.toUri().getPath()).getAbsoluteFile().toURI().toURL();
			localJarURLs.add(url);
		}
		return localJarURLs;
	}

	private static List<Path> jobGraphJarPathsToLocalize(final Map<String, String> environment) {
		final String configValue = environment.get(YarnConfigKeys.ENV_JAR_FILES);
		checkState(configValue != null, "At least the user jar must be included.");

		return Arrays.stream(configValue.split(","))
				.map(entry -> {
					final String hdfsPath = entry.split("=")[1];
					return new Path(hdfsPath);
				}).collect(Collectors.toList());
	}
}
