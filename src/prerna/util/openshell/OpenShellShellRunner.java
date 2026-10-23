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
package prerna.util.openshell;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import prerna.auth.User;
import prerna.util.OpenShellTransport;
import prerna.util.Utility;

/**
 * Runs shell commands <b>inside</b> the user's OpenShell sandbox.
 *
 * <p>
 * Why this exists: {@code CmdExecUtil} executes commands with Apache
 * {@code commons-exec} on Tomcat's own machine, inside a chroot. That is correct
 * for the legacy jail and wrong for a sandbox — measured 2026-10-22, a
 * {@code Command("hostname")} pixel returned the host's identity while
 * {@code Py()} returned the sandbox's. The terminal was simply not part of the
 * migration.
 *
 * <p>
 * The command is run with {@code openshell sandbox exec}, which starts a process
 * inside the sandbox. That is deliberately <b>not</b> the worker socket: the
 * worker serves one connection and a shell command must not compete with the
 * REPL for it. It also means shell output does not require the python worker to
 * be healthy.
 *
 * <p>
 * The sandbox is started on demand if it is not running yet, so a terminal
 * command works as the first action in a session rather than requiring a
 * {@code Py()} first.
 */
public final class OpenShellShellRunner {

	private static final int DEFAULT_TIMEOUT_SECONDS = 60;
	private static final String CLI = "openshell";

	private OpenShellShellRunner() {
		// static utility
	}

	/** Outcome of one shell command. */
	public static final class Result {
		private final String output;
		private final String workingDir;

		Result(String output, String workingDir) {
			this.output = output;
			this.workingDir = workingDir;
		}

		public String getOutput() {
			return output;
		}

		public String getWorkingDir() {
			return workingDir;
		}
	}

	/**
	 * Execute one command in the sandbox belonging to {@code user}.
	 *
	 * @param user       the session user; used to make sure the sandbox exists
	 * @param workingDir directory to run in (an absolute path inside the sandbox)
	 * @param command    the command line, as typed at the terminal
	 */
	public static Result execute(User user, String workingDir, String command) {
		String dir = workingDir == null || workingDir.trim().isEmpty() ? "/" : workingDir.trim();
		try {
			// Same on-demand rule as Py(): the first shell command in a session is
			// allowed to create the sandbox.
			user.getPythonSocketClient(true);
			String chrootFolder = user.getUserSymlinkHelper().getUserChrootFolder();
			String sandboxName = OpenShellTransport.sandboxNameFor(chrootFolder);

			String trimmed = command == null ? "" : command.trim();
			if (trimmed.isEmpty()) {
				return new Result("", dir);
			}
			if (equalsIgnoreCase(trimmed, "pwd")) {
				return new Result(dir, dir);
			}
			if (equalsIgnoreCase(trimmed, "reset")) {
				return new Result(dir, dir);
			}
			if (trimmed.startsWith("cd ") || trimmed.equals("cd")) {
				String target = trimmed.length() > 2 ? trimmed.substring(3).trim() : "/";
				if (target.equals("~")) {
					target = "/";
				}
				String resolved = runInSandbox(sandboxName, dir, "cd " + target + " && pwd");
				if (resolved == null || resolved.trim().isEmpty() || resolved.startsWith("__ERROR__")) {
					return new Result(resolved == null ? "Directory doesn't exist: " + target
							: resolved.replace("__ERROR__", ""), dir);
				}
				String newDir = firstLine(resolved);
				return new Result(newDir, newDir);
			}
			String output = runInSandbox(sandboxName, dir, trimmed);
			// Shell writes are not requests on the python socket, so they do not pass the
			// fetch-back hook there. Reconcile here too -- this is the path a `git commit`
			// or a `cat > file` takes, and losing that work is exactly what we are fixing.
			try {
				if (user.getPythonClientProcessWrapper() != null) {
					user.getPythonClientProcessWrapper().fetchBackChangedPaths();
				}
			} catch (Exception ignored) {
				// the command itself already ran; a failed reconcile must not fail the shell
			}
			return new Result(output, dir);
		} catch (Exception e) {
			return new Result("Shell execution in sandbox failed: " + e.getClass().getSimpleName() + ": "
					+ e.getMessage(), dir);
		}
	}

	private static boolean equalsIgnoreCase(String left, String right) {
		return left != null && left.equalsIgnoreCase(right);
	}

	private static String firstLine(String text) {
		int newline = text.indexOf('\n');
		return (newline < 0 ? text : text.substring(0, newline)).trim();
	}

	/**
	 * One `openshell sandbox exec` invocation.
	 *
	 * <p>
	 * {@code bash -lc} on purpose: the command string users type is a shell line
	 * (pipes, redirects, &&), and a login shell gives a predictable PATH inside the
	 * image. The working directory is applied by the shell itself rather than by
	 * {@code --workdir}, because the directory may be a path that only exists in
	 * the sandbox after an earlier upload.
	 */
	private static String runInSandbox(String sandboxName, String workingDir, String command) throws IOException {
		List<String> argv = new ArrayList<>(Arrays.asList(CLI, "sandbox", "exec", "-n", sandboxName, "--",
				"bash", "-lc", "cd " + shellQuote(workingDir) + " 2>/dev/null; " + command));
		ProcessBuilder builder = new ProcessBuilder(argv);
		builder.redirectErrorStream(true);
		builder.redirectInput(ProcessBuilder.Redirect.from(new java.io.File("/dev/null")));
		// The CLI needs the same environment the transport uses (gateway config,
		// TLS bundle); inherit it rather than reconstructing it here.
		Process process = builder.start();
		String output;
		try {
			if (!process.waitFor(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
				process.destroyForcibly();
				return "__ERROR__command timed out after " + DEFAULT_TIMEOUT_SECONDS + "s";
			}
			output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			process.destroyForcibly();
			return "__ERROR__interrupted";
		}
		String cleaned = output.replace("\\r", "").trim();
		if (process.exitValue() != 0 && !cleaned.isEmpty()) {
			return cleaned;
		}
		return cleaned;
	}

	private static String shellQuote(String value) {
		return "'" + value.replace("'", "'\\''") + "'";
	}

	/** Exposed for tests: the exact argv shape used for one command. */
	public static List<String> describeCommand(String sandboxName, String workingDir, String command) {
		return Arrays.asList(CLI, "sandbox", "exec", "-n", sandboxName, "--", "bash", "-lc",
				"cd " + shellQuote(workingDir) + " 2>/dev/null; " + command);
	}

	/** Kept for symmetry with the transport's property handling. */
	static String cliPath() {
		String configured = Utility.getDIHelperProperty(OpenShellTransport.CLI_PROP);
		return configured == null || configured.trim().isEmpty() ? CLI : configured.trim();
	}
}
