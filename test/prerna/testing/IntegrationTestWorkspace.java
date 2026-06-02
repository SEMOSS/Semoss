/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.testing;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Per-JVM temporary directory used as the base folder for the integration
 * tests that extend {@link AbstractBaseSemossApiTests}.
 *
 * Replaces the old committed {@code testfolder/} layout. On first access the
 * workspace:
 * <ol>
 *   <li>creates a temp dir named {@code semoss-it-<random>} via
 *       {@link Files#createTempDirectory(String, java.nio.file.attribute.FileAttribute...)};</li>
 *   <li>copies the read-only fixture tree from
 *       {@code test/resources/testbase/} into it (db SMSS files, testconfig
 *       allow-lists, social.properties, emailTemplates);</li>
 *   <li>copies the {@code test/resources/mailpit/} fixture (README and any
 *       user-installed binary) into the workspace;</li>
 *   <li>creates the empty engine-type root dirs that
 *       {@code ensureTestFolderStructure} used to make
 *       (project, function, model, storage, vector, venv, user, InsightCache);</li>
 *   <li>registers a JVM shutdown hook that deletes the whole workspace.</li>
 * </ol>
 *
 * The marker {@link #MARKER_PREFIX} is used by the cleanup code in
 * {@link ApiSemossTestEngineUtils} as a tripwire to make sure we never clear
 * tables in a non-test database.
 */
public final class IntegrationTestWorkspace {

	private static final Logger classLogger = LogManager.getLogger(IntegrationTestWorkspace.class);

	public static final String MARKER_PREFIX = "semoss-it-";

	private static final String FIXTURE_TESTBASE = "test/resources/testbase";

	private static final List<String> RUNTIME_SUBDIRS = Arrays.asList("project", "function", "model", "storage",
			"vector", "venv", "user", "InsightCache");

	private static volatile Path basePath;

	private IntegrationTestWorkspace() {
	}

	public static Path basePath() {
		Path p = basePath;
		if (p == null) {
			synchronized (IntegrationTestWorkspace.class) {
				p = basePath;
				if (p == null) {
					p = initialize();
					basePath = p;
				}
			}
		}
		return p;
	}

	public static String marker() {
		return MARKER_PREFIX;
	}

	private static Path initialize() {
		try {
			Path tempBase = Files.createTempDirectory(MARKER_PREFIX);
			classLogger.info("Integration test workspace: {}", tempBase);

			copyFixtures(tempBase);
			createRuntimeSubdirs(tempBase);
			registerShutdownHook(tempBase);

			return tempBase;
		} catch (IOException e) {
			throw new UncheckedIOException("Failed to initialize integration test workspace", e);
		}
	}

	private static void copyFixtures(Path tempBase) throws IOException {
		Path repoRoot = Paths.get("").toAbsolutePath();
		Path testbaseFixture = repoRoot.resolve(FIXTURE_TESTBASE);

		if (!Files.isDirectory(testbaseFixture)) {
			throw new IOException("testbase fixture directory not found at " + testbaseFixture);
		}

		FileUtils.copyDirectory(testbaseFixture.toFile(), tempBase.toFile());
		classLogger.info("Copied testbase fixtures from {} to {}", testbaseFixture, tempBase);
		// Mailpit deliberately not copied: its exe path needs to stay stable so the
		// Windows firewall rule the user grants on first run keeps applying. See
		// ApiSemossTestEmailUtils for where the stable path is read from.
	}

	private static void createRuntimeSubdirs(Path tempBase) throws IOException {
		for (String sub : RUNTIME_SUBDIRS) {
			Path p = tempBase.resolve(sub);
			if (!Files.isDirectory(p)) {
				Files.createDirectories(p);
			}
		}
	}

	private static void registerShutdownHook(Path tempBase) {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try (Stream<Path> walk = Files.walk(tempBase)) {
				walk.sorted((a, b) -> b.getNameCount() - a.getNameCount()).map(Path::toFile).forEach(File::delete);
			} catch (IOException e) {
				classLogger.warn("Could not fully clean up workspace {}: {}", tempBase, e.toString());
			}
		}, "integration-test-workspace-cleanup"));
	}
}
