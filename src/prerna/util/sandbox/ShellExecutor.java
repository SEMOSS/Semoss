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
package prerna.util.sandbox;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.input.NullInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.Utility;

/**
 * Runs one shell command to completion, on the host or inside a user's fakechroot. This is the
 * code that lived in {@code CmdExecUtil.runCommand}, unchanged in behavior.
 */
final class ShellExecutor {

	private static final Logger classLogger = LogManager.getLogger(ShellExecutor.class);
	private static final String DEFAULT_CHROOT_PATH = String.join(File.pathSeparator, "/usr/local/sbin",
			"/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin");

	private ShellExecutor() {
		// static utility
	}

	/** The shell the host runs commands with: bash, or cmd or powershell on Windows. */
	private static String shell() {
		String osName = System.getProperty("os.name").toLowerCase();
		if (osName.indexOf("win") >= 0) {
			return "cmd".equals(Utility.getDefaultTerminalMode()) ? "cmd" : "powershell.exe";
		}
		return "/bin/bash";
	}

	/** Runs a command on the host, with no jail. */
	static ExecResult runOnHost(String workingDir, String command, int timeoutSeconds) {
		String shell = shell();
		CommandLine cmdLine = new CommandLine(shell);
		cmdLine.addArgument(shell.equalsIgnoreCase("/bin/bash") ? "-c" : "/C");
		cmdLine.addArgument(command, false);
		return run(cmdLine, new File(Utility.normalizePath(workingDir)), createEnvironmentWithConfiguredPythonPath(),
				timeoutSeconds);
	}

	/** Runs a command in a user's fakechroot. */
	static ExecResult runInChroot(String chrootFolder, String workingDir, String command, int timeoutSeconds) {
		File chrootDir = new File(chrootFolder);
		if (!chrootDir.exists()) {
			classLogger.error("Chroot directory does not exist: " + chrootFolder);
			return new ExecResult(false, "Chroot directory does not exist: " + chrootFolder, null);
		}
		File bashInChroot = new File(chrootDir, "bin/bash");
		if (!bashInChroot.exists()) {
			classLogger.error("Bash does not exist in chroot: " + bashInChroot.getAbsolutePath());
			return new ExecResult(false, "Bash does not exist in chroot: " + bashInChroot.getAbsolutePath(), null);
		}

		// Build the complete command as a single string argument
		String escapedWorkingDir = workingDir.replace("'", "'\"'\"'"); // Proper shell escaping
		CommandLine cmdLine = new CommandLine("fakechroot");
		cmdLine.addArgument("fakeroot");
		cmdLine.addArgument("chroot");
		cmdLine.addArgument("--userspec=1001:1001");
		cmdLine.addArgument(chrootFolder);
		cmdLine.addArgument(shell());
		cmdLine.addArgument("-c");
		cmdLine.addArgument("cd '" + escapedWorkingDir + "' && " + command, false);
		// For chroot, the working directory is the chroot base, not the target directory
		return run(cmdLine, chrootDir, createChrootEnvironment(), timeoutSeconds);
	}

	private static ExecResult run(CommandLine cmdLine, File workingDirectory, Map<String, String> environment,
			int timeoutSeconds) {
		ExecuteWatchdog.Builder executeWatchdogBuilder = ExecuteWatchdog.builder();
		executeWatchdogBuilder.setTimeout(Duration.ofSeconds(timeoutSeconds));
		DefaultExecutor.Builder<?> executorBuilder = DefaultExecutor.builder();
		executorBuilder.setWorkingDirectory(workingDirectory);

		classLogger.info("Running command executable: " + cmdLine.getExecutable());
		classLogger.info("Complete command: " + cmdLine);

		boolean success = true;
		try (ByteArrayOutputStream stdout = new ByteArrayOutputStream();
				ByteArrayOutputStream stderr = new ByteArrayOutputStream()) {
			DefaultExecutor executor = executorBuilder.get();

			PumpStreamHandler streamHandler = new PumpStreamHandler(stdout, stderr, NullInputStream.nullInputStream());
			executor.setStreamHandler(streamHandler);

			ExecuteWatchdog watchdog = executeWatchdogBuilder.get();
			executor.setWatchdog(watchdog);

			int exitValue = -1;
			try {
				if (environment == null) {
					exitValue = executor.execute(cmdLine);
				} else {
					exitValue = executor.execute(cmdLine, environment);
				}
				classLogger.debug("Command executed successfully with exit code: " + exitValue);
			} catch (Exception ex) {
				success = false;
				classLogger.error("Command execution failed with exit code: " + exitValue, ex);
			}

			// Combine stdout and stderr
			String stdoutStr = stdout.toString();
			String stderrStr = stderr.toString();
			String output = stdoutStr + stderrStr;
			output = output.trim().replace("\\", "/");

			if (!stdoutStr.isEmpty()) {
				classLogger.debug("Command stdout: " + stdoutStr);
			}
			if (!stderrStr.isEmpty()) {
				classLogger.debug("Command stderr: " + stderrStr);
			}

			// If command failed but we don't have error output, add exit code info
			if (!success && output.isEmpty()) {
				output = "Command failed with exit code: " + exitValue;
			}
			return new ExecResult(success, output, null);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return new ExecResult(false, "IO Exception occurred: " + e.getMessage(), null);
		}
	}

	private static Map<String, String> createEnvironmentWithConfiguredPythonPath() {
		String pythonHome = System.getenv(Settings.PYTHONHOME);
		if (pythonHome == null || pythonHome.trim().isEmpty()) {
			pythonHome = Utility.getDIHelperProperty(Settings.PYTHONHOME);
		}
		if (pythonHome == null || pythonHome.trim().isEmpty()) {
			pythonHome = System.getenv(Settings.PY_HOME);
		}
		if (pythonHome == null || pythonHome.trim().isEmpty()) {
			pythonHome = Utility.getDIHelperProperty(Settings.PY_HOME);
		}
		String sandboxPath = getConfiguredSandboxPath();
		if ((pythonHome == null || pythonHome.trim().isEmpty()) && sandboxPath.isEmpty()) {
			return null;
		}

		Map<String, String> environment = new HashMap<>(System.getenv());
		String path = prependPath(sandboxPath, environment.get("PATH"));
		if (pythonHome != null && !pythonHome.trim().isEmpty()) {
			String pythonBin = new File(pythonHome.trim(), "bin").getAbsolutePath();
			path = prependPath(pythonBin, path);
		}
		environment.put("PATH", path);
		return environment;
	}

	private static Map<String, String> createChrootEnvironment() {
		Map<String, String> environment = new HashMap<>();
		environment.put("HOME", "/home/default");
		environment.put("PATH", prependPath(getConfiguredSandboxPath(), DEFAULT_CHROOT_PATH));
		return environment;
	}

	private static String getConfiguredSandboxPath() {
		return SandboxSettings.SANDBOX_PATH.configured() == null ? "" : SandboxSettings.SANDBOX_PATH.configured();
	}

	private static String prependPath(String prefix, String existingPath) {
		Set<String> entries = new LinkedHashSet<>();
		addPathEntries(entries, prefix);
		addPathEntries(entries, existingPath);
		return String.join(File.pathSeparator, entries);
	}

	private static void addPathEntries(Set<String> entries, String path) {
		if (path == null || path.trim().isEmpty()) {
			return;
		}
		for (String entry : path.split(File.pathSeparator)) {
			String trimmed = entry.trim();
			if (!trimmed.isEmpty()) {
				entries.add(trimmed);
			}
		}
	}
}
