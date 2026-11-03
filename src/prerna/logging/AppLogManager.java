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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.action.Action;
import org.apache.logging.log4j.core.appender.rolling.action.DeleteAction;
import org.apache.logging.log4j.core.appender.rolling.action.IfAccumulatedFileCount;
import org.apache.logging.log4j.core.appender.rolling.action.IfFileName;
import org.apache.logging.log4j.core.appender.rolling.action.PathCondition;
import org.apache.logging.log4j.core.appender.rolling.action.PathSortByModificationTime;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;

import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Manages per-project Log4j2 {@link RollingFileAppender} instances.
 * <p>
 * When an app is first loaded (via {@code Insight.setContext()}), this class
 * registers a {@link RollingFileAppender} for the project that is filtered by
 * the {@code projectId} MDC key set in {@link prerna.sablecc2.comm.PixelJobRunner}.
 * Only application-owned loggers and explicit {@code LogMessage} output whose
 * MDC {@code projectId} matches the project are written. SEMOSS framework and
 * engine telemetry are excluded so opening or searching the log does not log
 * itself.
 * <p>
 * Log4j2's own {@link Configuration} is the single source of truth for whether
 * an appender has been registered. This means appenders are automatically
 * re-registered if Log4j2 reloads its configuration at runtime.
 * <p>
 * Log files are written outside project content so project synchronization,
 * publication, and Git operations never copy runtime logs.
 */
public final class AppLogManager {

	private static final Logger classLogger = LogManager.getLogger(AppLogManager.class);
	private static final String APP_LOG_DIRECTORY_PROPERTY = "APP_LOG_DIRECTORY";
	private static final String APP_LOG_MAX_FILE_SIZE_PROPERTY = "APP_LOG_MAX_FILE_SIZE";
	private static final String APP_LOG_MAX_FILES_PROPERTY = "APP_LOG_MAX_FILES";
	private static final String DEFAULT_MAX_FILE_SIZE = "10MB";
	private static final int DEFAULT_MAX_FILES = 5;
	private static final int MAX_CONFIGURED_FILES = 20;
	private static final Pattern FILE_SIZE_PATTERN = Pattern.compile("(?i)^\\d+\\s*(KB|MB|GB)$");
	private static final Pattern ROTATED_LOG_FILE_PATTERN =
			Pattern.compile("^app\\.log\\.(?:\\d+|\\d{4}-\\d{2}-\\d{2}\\.\\d+)$");
	private static final String ROTATED_LOG_FILE_REGEX =
			"app\\.log\\.(?:\\d+|\\d{4}-\\d{2}-\\d{2}\\.\\d+)";

	/** Log pattern used for per-project appenders - mirrors the global file appender. */
	private static final String LOG_PATTERN =
			"[%-5level] %d{yyyy-MM-dd HH:mm:ss} %c{1.}:%L [user=%X{userId}] "
					+ "%maxLen{%maskMsg}{65536}%n";

	/**
	 * Loggers that should write to per-project files (mirrors log4j2.xml
	 * AsyncLoggers), plus the root logger - custom app reactors commonly live
	 * in packages outside "prerna" (e.g. a VA/CFG app's own "reactors.*"
	 * namespace) and fall through to root rather than inheriting from
	 * "prerna" or "EngineLogger", which are separate top-level namespaces,
	 * not ancestors of an arbitrary custom package.
	 */
	private static final String[] TARGET_LOGGERS = { "prerna", "EngineLogger", LogManager.ROOT_LOGGER_NAME };

	private AppLogManager() {
		// utility class - no instances
	}

	/**
	 * Ensures a per-project log appender is registered for the given project.
	 * <p>
	 * Uses the Log4j2 {@link Configuration} as the single source of truth -
	 * no separate tracking set. Handles Log4j2 config reloads automatically:
	 * if the config is refreshed and the appender is lost, the next
	 * {@code setContext()} call will re-register it.
	 * <p>
	 * Idempotent - safe to call on every {@code setContext()} invocation.
	 *
	 * @param projectId   the project whose logs should be captured
	 * @param projectName the project display name retained for caller compatibility
	 */
	public static void ensureAppender(String projectId, String projectName) {
		if (!isEnabled()) {
			return;
		}
		if (projectId == null || projectId.isBlank()) {
			return;
		}
		String appenderName = "AppFile-" + projectId;
		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);

		// Fast path - appender already registered in the current config
		if (ctx.getConfiguration().getAppender(appenderName) != null) {
			return;
		}

		// Slow path - register under a class-level lock to prevent concurrent
		// double-registration when multiple threads load the same app simultaneously
		synchronized (AppLogManager.class) {
			if (ctx.getConfiguration().getAppender(appenderName) != null) {
				return; // another thread registered it while we waited
			}
			try {
				registerAppender(projectId, projectName, appenderName, ctx);
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
	 * @return absolute path, e.g. {@code .../logs/apps/{projectId}/app.log}
	 */
	public static String getLogFilePath(String projectId, String projectName) {
		return getLogDirectory() + File.separator + projectId + File.separator + "app.log";
	}

	/**
	 * Returns the active file followed by retained archives, newest first.
	 * Legacy numeric archives remain searchable during migration to dated names.
	 */
	static List<File> getLogFiles(String projectId, String projectName) {
		String basePath = getLogFilePath(projectId, projectName);
		File active = new File(basePath);
		File directory = active.getParentFile();
		List<File> files = new ArrayList<>();
		if (active.exists()) {
			files.add(active);
		}
		if (directory == null || !directory.isDirectory()) {
			return files;
		}

		File[] archives = directory.listFiles(file ->
				file.isFile() && ROTATED_LOG_FILE_PATTERN.matcher(file.getName()).matches());
		if (archives == null) {
			return files;
		}
		List<File> sortedArchives = new ArrayList<>(List.of(archives));
		sortedArchives.sort(Comparator.comparingLong(File::lastModified)
				.reversed()
				.thenComparing(File::getName));
		files.addAll(sortedArchives.subList(0, Math.min(sortedArchives.size(), getMaxFiles())));
		return files;
	}

	/**
	 * Global operational gate. Application logging is opt-in so a missing or
	 * invalid property never creates or writes project log files.
	 */
	public static boolean isEnabled() {
		String configured = Utility.getDIHelperProperty(Constants.APP_LOGGING_ENABLED);
		return Boolean.parseBoolean(configured);
	}

	// -- private --------------------------------------------------------------

	private static void registerAppender(String projectId, String projectName,
			String appenderName, LoggerContext ctx) throws IOException {
		String logFile = getLogFilePath(projectId, projectName);
		File logDirFile = new File(logFile).getParentFile();

		if (!logDirFile.exists() && !logDirFile.mkdirs()) {
			classLogger.warn("Could not create log directory '{}' for project '{}'", logDirFile, projectId);
		}

		Configuration config = ctx.getConfiguration();

		Filter filter = new ProjectAppLogFilter(projectId);

		PatternLayout layout = PatternLayout.newBuilder()
				.withPattern(LOG_PATTERN)
				.withConfiguration(config)
				.build();

		PathCondition archiveRetention = IfFileName.createNameCondition(null, ROTATED_LOG_FILE_REGEX,
				IfAccumulatedFileCount.createFileCountCondition(getMaxFiles()));
		Action deleteOldArchives = DeleteAction.createDeleteAction(
				logDirFile.getAbsolutePath(),
				false,
				1,
				false,
				PathSortByModificationTime.createSorter(true),
				new PathCondition[] { archiveRetention },
				null,
				config);

		RollingFileAppender appender = RollingFileAppender.newBuilder()
				.withName(appenderName)
				.withFileName(logFile)
				.withFilePattern(logFile + ".%d{yyyy-MM-dd}.%i")
				.withAppend(true)
				.withLayout(layout)
				.withFilter(filter)
				.withPolicy(CompositeTriggeringPolicy.createPolicy(
						SizeBasedTriggeringPolicy.createPolicy(getMaxFileSize()),
						TimeBasedTriggeringPolicy.newBuilder()
								.withInterval(1)
								.withModulate(true)
								.build()))
				.withStrategy(DefaultRolloverStrategy.newBuilder()
						.withMax(Integer.toString(getMaxFiles()))
						.withCustomActions(new Action[] { deleteOldArchives })
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

		classLogger.info("Registered per-project log appender for '{}' -> {}", projectId, logFile);
	}

	private static String getLogDirectory() {
		String configured = Utility.getDIHelperProperty(APP_LOG_DIRECTORY_PROPERTY);
		if (configured != null && !configured.isBlank()) {
			return Utility.normalizePath(configured.trim());
		}
		String catalinaBase = System.getProperty("catalina.base");
		String base = catalinaBase == null || catalinaBase.isBlank()
				? Utility.getBaseFolder() + File.separator + "logs"
				: catalinaBase.trim() + File.separator + "logs";
		return Utility.normalizePath(base + File.separator + "apps");
	}

	private static String getMaxFileSize() {
		String configured = Utility.getDIHelperProperty(APP_LOG_MAX_FILE_SIZE_PROPERTY);
		if (configured != null && FILE_SIZE_PATTERN.matcher(configured.trim()).matches()) {
			return configured.trim().toUpperCase();
		}
		return DEFAULT_MAX_FILE_SIZE;
	}

	static int getMaxFiles() {
		String configured = Utility.getDIHelperProperty(APP_LOG_MAX_FILES_PROPERTY);
		if (configured == null || configured.isBlank()) {
			return DEFAULT_MAX_FILES;
		}
		try {
			return Math.max(1, Math.min(Integer.parseInt(configured.trim()), MAX_CONFIGURED_FILES));
		} catch (NumberFormatException e) {
			classLogger.warn("Invalid {} value '{}'; using {}", APP_LOG_MAX_FILES_PROPERTY, configured,
					DEFAULT_MAX_FILES);
			return DEFAULT_MAX_FILES;
		}
	}

	static boolean isApplicationLogger(String loggerName) {
		if ("prerna.reactor.LogMessage".equals(loggerName)) {
			return true;
		}
		return loggerName == null
				|| (!loggerName.startsWith("prerna.") && !"EngineLogger".equals(loggerName));
	}

	static boolean shouldCaptureEvent(String projectId, String eventProjectId, String loggerName) {
		return isEnabled()
				&& projectId.equals(eventProjectId)
				&& isApplicationLogger(loggerName);
	}

	private static final class ProjectAppLogFilter extends AbstractFilter {

		private final String projectId;

		private ProjectAppLogFilter(String projectId) {
			super(Result.ACCEPT, Result.DENY);
			this.projectId = projectId;
		}

		@Override
		public Result filter(LogEvent event) {
			String eventProjectId = event.getContextData().getValue("projectId");
			return shouldCaptureEvent(projectId, eventProjectId, event.getLoggerName())
					? Result.ACCEPT
					: Result.DENY;
		}
	}
}
