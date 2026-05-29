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
package prerna.reactor.agent.sandbox;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.input.NullInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;

/**
 * Runs a Command-reactor shell command in a Landlock-confined, fakechroot-wrapped subprocess.
 * Linux + chroot only — non-chroot sessions are rejected by the Command reactor before
 * reaching here. Writes are restricted to the room folder; reads span the chroot root.
 */
public final class CmdSandboxLauncher {

	private static final Logger classLogger = LogManager.getLogger(CmdSandboxLauncher.class);

	private static final int COMMAND_TIMEOUT_SECONDS = 30;

	private CmdSandboxLauncher() {
	}

	/**
	 * Build a Landlock policy: RW on {@code roomFolder}, RO on the shell runtime and
	 * every chroot ancestor required for path traversal.
	 *
	 * @throws IllegalStateException on non-Linux
	 * @throws IllegalArgumentException if {@code chrootPath} is empty
	 */
	public static SandboxPolicy buildRoomCommandPolicy(String roomFolder, String chrootPath) {
		if (Platform.current() != Platform.LINUX) {
			throw new IllegalStateException(
					"Command reactor sandboxing is supported only on Linux; current platform="
							+ Platform.current());
		}
		if (chrootPath == null || chrootPath.trim().isEmpty()) {
			throw new IllegalArgumentException(
					"Command reactor sandboxing requires a chroot path");
		}

		SandboxPolicyBuilder b = SandboxPolicy.builder()
				.withEnforcement(AgentSandboxConfig.resolveEnforcement())
				.withLoopbackNetwork(false)
				.withReadWrite(roomFolder);

		// Landlock: explicit RO allowlist required (no global read rule).
		b.withRead("/bin");
		b.withRead("/usr/bin");
		b.withRead("/usr/lib");
		b.withRead("/usr/lib64");
		b.withRead("/lib");
		b.withRead("/lib64");
		b.withRead("/etc/ssl");
		b.withRead(System.getProperty("java.home"));

		// fakechroot needs to traverse the chroot root for path mapping
		b.withRead(chrootPath);
		// Landlock requires READ_DIR on every ancestor of roomFolder for traversal
		Path ancestor = Paths.get(roomFolder).getParent();
		while (ancestor != null && ancestor.getNameCount() > 0) {
			b.withRead(ancestor.toString());
			ancestor = ancestor.getParent();
		}

		return b.build();
	}

	/**
	 * Execute {@code command} via {@code fakechroot fakeroot chroot --userspec=1001:1001 /bin/bash -c "cd {workingDir} && ..."}
	 * with {@code policy} enforced by Landlock.
	 *
	 * @return {@code [success?, output]}, or {@code null} if Landlock isn't available on this host
	 * @throws IllegalStateException on non-Linux
	 * @throws IllegalArgumentException if {@code chrootPath} is empty
	 */
	public static String[] execute(SandboxPolicy policy, String workingDir,
			String command, String chrootPath) {
		if (Platform.current() != Platform.LINUX) {
			throw new IllegalStateException(
					"Command reactor sandboxing is supported only on Linux; current platform="
							+ Platform.current());
		}
		if (chrootPath == null || chrootPath.trim().isEmpty()) {
			throw new IllegalArgumentException(
					"Command reactor sandboxing requires a chroot path");
		}

		SandboxLauncher launcher;
		try {
			launcher = SandboxLauncherRegistry.get();
		} catch (SandboxUnavailableException e) {
			classLogger.warn("Landlock backend unavailable on this host; "
					+ "falling back to Layer-1 navigation confinement only: {}", e.getMessage());
			return null;
		}

		String escapedDir = workingDir.replace("'", "'\"'\"'");
		String shellCmd = "cd '" + escapedDir + "' && " + command;
		String targetBinary = "/usr/bin/fakechroot";
		String[] targetArgs = new String[] {
				"fakeroot", "chroot", "--userspec=1001:1001", chrootPath,
				"/bin/bash", "-c", shellCmd
		};

		SandboxLaunchPlan plan;
		try {
			plan = launcher.plan(policy, targetBinary, targetArgs);
		} catch (Exception e) {
			classLogger.error("Failed to build sandbox launch plan: {}", e.getMessage(), e);
			return null;
		}

		return runSandboxed(plan, targetArgs);
	}

	/** Spawn the plan and capture combined stdout+stderr into {@code [success?, output]}. */
	private static String[] runSandboxed(SandboxLaunchPlan plan, String[] targetArgs) {
		String[] foutput = new String[2];

		// argv[0] = wrapper script; plan.cliArgs is always empty for both launchers,
		// but we respect it for forward-compatibility. targetArgs follow.
		CommandLine cmdLine = new CommandLine(plan.getCliPath());
		for (String arg : plan.getCliArgs()) {
			cmdLine.addArgument(arg, false);
		}
		for (String arg : targetArgs) {
			cmdLine.addArgument(arg, false);
		}

		// Merge current environment with launcher additions/removals
		Map<String, String> env = new HashMap<>(System.getenv());
		env.putAll(plan.getEnvironmentAdditions());
		plan.getEnvironmentRemovals().forEach(env::remove);

		ExecuteWatchdog watchdog = ExecuteWatchdog.builder()
				.setTimeout(Duration.ofSeconds(COMMAND_TIMEOUT_SECONDS))
				.get();
		DefaultExecutor executor = DefaultExecutor.builder().get();
		executor.setWatchdog(watchdog);

		try (ByteArrayOutputStream stdout = new ByteArrayOutputStream();
				ByteArrayOutputStream stderr = new ByteArrayOutputStream()) {

			executor.setStreamHandler(
					new PumpStreamHandler(stdout, stderr, NullInputStream.nullInputStream()));

			boolean success = true;
			try {
				executor.execute(cmdLine, env);
			} catch (Exception ex) {
				success = false;
				classLogger.debug("Sandboxed command exited non-zero: {}", ex.getMessage());
			}

			String out = (stdout.toString() + stderr.toString())
					.trim().replace("\\", "/");

			foutput[0] = String.valueOf(success);
			foutput[1] = out.isEmpty()
					? (success ? "Command executed successfully" : "Command failed with no output")
					: out;

		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			foutput[0] = "false";
			foutput[1] = "IO Exception: " + e.getMessage();
		}

		return foutput;
	}
}
