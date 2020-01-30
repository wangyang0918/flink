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

package org.apache.flink.yarn.entrypoint.application;

import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Javadoc.
 */
public class Utils {

	public static PackagedProgram getPackagedProgram(final Configuration configuration) throws FileNotFoundException, ProgramInvocationException {
		checkNotNull(configuration);

		final ExecutionConfigAccessor configAccessor =
				ExecutionConfigAccessor.fromConfiguration(configuration);

		final List<URL> jars = configAccessor.getJars();
		checkState(!jars.isEmpty(), "At least the job jar should be specified.");

		final List<URL> classpaths = configAccessor.getClasspaths();
		final String entryPointClass = configAccessor.getMainClassName();
		final String[] programArgs = configAccessor.getProgramArgs().toArray(new String[0]);

		final SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.fromConfiguration(configuration);
		final File jar = getJarFile(jars.get(0)); // TODO: 30.01.20 is this correct? the user may specify multiple jars but we keep one???

		return PackagedProgram.newBuilder()
				.setJarFile(jar)
				.setUserClassPaths(classpaths)
				.setEntryPointClassName(entryPointClass)
				.setConfiguration(configuration)
				.setSavepointRestoreSettings(savepointRestoreSettings)
				.setArguments(programArgs)
				.build();
	}

	private static File getJarFile(final URL jarFileUrl) throws FileNotFoundException {
		File jarFile = new File(jarFileUrl.toString());
		if (!jarFile.exists()) {
			throw new FileNotFoundException("JAR file does not exist: " + jarFile);
		}
		else if (!jarFile.isFile()) {
			throw new FileNotFoundException("JAR file is not a file: " + jarFile);
		}
		return jarFile;
	}
}
