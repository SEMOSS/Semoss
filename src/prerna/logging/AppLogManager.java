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
package prerna.logging;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.filter.ThreadContextMapFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.util.KeyValuePair;

import prerna.util.AssetUtility;

/**
 * Manages per-project Log4j2 {@link RollingFileAppender} instances.
 * <p>
 * When an app is first loaded (via {@code Insight.setContext()}), this class
 * registers a {@link RollingFileAppender} for the project that is filtered by
 * the {@code projectId} MDC key set in {@link prerna.sablecc2.comm.PixelJobRunner}.
 * Only log events whose MDC {@code projectId} matches the project are written
 * to that project's log file — all other events are silently dropped by the
 * {@link ThreadContextMapFilter}.
 * <p>
 * Log files are written to:
 * <pre>{projectAssetsFolder}/logs/app.log</pre>
 * The {@code logs/} directory is added to the project's {@code .gitignore} to
 * prevent log files from being committed to git and causing pod filespace issues.
 */
public final class AppLogManager {

	private static final Logger classLogger = LogManager.getLogger(AppLogManager.class);

	/** Log pattern used for per-project appenders — mirrors the global file appender. */
	private static final String LOG_PATTERN =
			"[%-5level] %d{yyyy-MM-dd HH:mm:ss} %c{1.}:%L [user=%X{userId}] %maskMsg%n";

	/** Loggers that should write to per-project files (mirrors log4j2.xml AsyncLoggers). */
	private static final String[] TARGET_LOGGERS = { "prerna", "EngineLogger" };

	/** Tracks projects that already have an appender to avoid duplicate registration. */
	private static final Set<String> REGISTERED = ConcurrentHashMap.newKeySet();

	private AppLogManager() {
		// utility class — no instances
	}

	/**
	 * Ensures a per-project log appender is registered for the given project.
	 * <p>
	 * Idempotent and thread-safe — safe to call on every {@code setContext()} invocation.
	 * If the appender is already registered for this {@code projectId}, this is a no-op.
	 *
	 * @param projectId   the project whose logs should be captured
	 * @param projectName the project display name used to resolve the assets path
	 */
	public static void ensureAppender(String projectId, String projectName) {
		if (projectId == null || projectId.isBlank() || REGISTERED.contains(projectId)) {
			return;
		}
		synchronized (REGISTERED) {
			if (REGISTERED.contains(projectId)) {
				return; // double-checked inside lock
			}
			try {
				registerAppender(projectId, projectName);
				REGISTERED.add(projectId);
			} catch (Exception e) {
				classLogger.warn("Failed to register per-project log appender for '{}': {}",
						projectId, e.getMessage(), e);
			}
		}
	}

	/**
	 * Returns the path of the per-project log file.
	 * The file may not yet exist if the appender has not been registered.
	 *
	 * @param projectId   the project ID
	 * @param projectName the project display name
	 * @return absolute path, e.g. {@code .../version/logs/app.log}
	 */
	public static String getLogFilePath(String projectId, String projectName) {
		return AssetUtility.getProjectVersionFolder(projectName, projectId) + "/logs/app.log";
	}

	// ── private ────────────────────────────────────────────────────────────

	private static void registerAppender(String projectId, String projectName) throws IOException {
		// Write to version/logs/ — sits alongside assets/ so editors browsing the
		// assets folder never see log files, but it's still within the project tree.
		String versionFolder = AssetUtility.getProjectVersionFolder(projectName, projectId);
		String logDir = versionFolder + "/logs";
		String logFile = logDir + "/app.log";

		// Create version/logs/ directory if it doesn't exist
		File logDirFile = new File(logDir);
		if (!logDirFile.exists() && !logDirFile.mkdirs()) {
			classLogger.warn("Could not create log directory '{}' for project '{}'", logDir, projectId);
		}

		// Ensure logs/ is gitignored — .gitignore is at the version folder root
		ensureLogsGitIgnored(versionFolder);

		// Build the appender
		String appenderName = "AppFile-" + projectId;
		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		Configuration config = ctx.getConfiguration();

		// Guard against duplicate registration after a config reload
		if (config.getAppender(appenderName) != null) {
			return;
		}

		// Filter: ACCEPT only events where MDC projectId == this project's ID
		ThreadContextMapFilter filter = ThreadContextMapFilter.createFilter(
				new KeyValuePair[] { new KeyValuePair("projectId", projectId) },
				"and", Filter.Result.ACCEPT, Filter.Result.DENY);

		PatternLayout layout = PatternLayout.newBuilder()
				.withPattern(LOG_PATTERN)
				.withConfiguration(config)
				.build();

		RollingFileAppender appender = RollingFileAppender.newBuilder()
				.withName(appenderName)
				.withFileName(logFile)
				.withFilePattern(logFile + ".%i")
				.withAppend(true)
				.withLayout(layout)
				.withFilter(filter)
				.withPolicy(SizeBasedTriggeringPolicy.createPolicy("50MB"))
				.withStrategy(DefaultRolloverStrategy.newBuilder()
						.withMax("10")
						.withConfig(config)
						.build())
				.setConfiguration(config)
				.build();

		appender.start();
		if (!appender.isStarted()) {
			throw new IOException(
					"Log4j2 appender for project '" + projectId + "' failed to start. "
							+ "Check that '" + logFile + "' is writable.");
		}
		config.addAppender(appender);

		// Attach to the same loggers used in log4j2.xml
		for (String loggerName : TARGET_LOGGERS) {
			config.getLoggerConfig(loggerName).addAppender(appender, null, null);
		}
		ctx.updateLoggers();

		classLogger.info("Registered per-project log appender for '{}' → {}", projectId, logFile);
	}

	/**
	 * Appends {@code logs/} to the project's {@code .gitignore} if not already present.
	 * The {@code .gitignore} lives at the version folder root (next to {@code assets/}).
	 */
	private static void ensureLogsGitIgnored(String versionFolder) {
		try {
			File gitignore = new File(versionFolder, ".gitignore");

			if (gitignore.exists()) {
				String content = new String(Files.readAllBytes(gitignore.toPath()), StandardCharsets.UTF_8);
				if (content.contains("logs/")) {
					return;
				}
			}

			try (FileWriter fw = new FileWriter(gitignore, true);
					BufferedWriter bw = new BufferedWriter(fw)) {
				bw.newLine();
				bw.write("logs/");
				bw.newLine();
			}
		} catch (IOException e) {
			classLogger.warn("Could not update .gitignore for project at '{}': {}", versionFolder, e.getMessage());
		}
	}
}
