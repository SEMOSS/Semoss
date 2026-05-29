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
package prerna.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
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

import prerna.auth.User;
import prerna.reactor.agent.sandbox.CmdSandboxLauncher;
import prerna.reactor.agent.sandbox.SandboxPolicy;

public class CmdExecUtil {

	private static final Logger classLogger = LogManager.getLogger(CmdExecUtil.class);

	private String insightId = null;
	private String chrootFolderPath = null;
	private String workingDir = null;
	private String contextDir = null;
	private String commandAppender = "cmd";

	/** When set, cd navigation cannot leave this directory tree (Layer 1). */
	private String confinementRoot = null;

	/** When set, non-builtin commands are executed inside this sandbox (Layer 2). */
	private SandboxPolicy sandboxPolicy = null;

	/**
	 * Constructor
	 */
	public CmdExecUtil(User user, String insightId, String contextDir) {
		if (user != null && Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
			SymlinkHelper chrootHelper = user.getUserSymlinkHelper();
			this.chrootFolderPath = chrootHelper.getUserChrootFolder();
		}
		getCommandAppender();
		this.insightId = insightId;
		if (contextDir == null) {
			contextDir = "/";
		}
		this.contextDir = contextDir.replace("\\", "/");
		this.workingDir = this.contextDir;
		classLogger.info("Insight id / Project " + insightId + " set at working directory " + workingDir);
	}

	/**
	 * Convert a chroot-relative path to the actual system path for file operations
	 * 
	 * @param chrootRelativePath Path as it appears inside the chroot
	 * @return Actual system path for file operations
	 */
	private String chrootToSystemPath(String chrootRelativePath) {
		if (this.chrootFolderPath == null || this.chrootFolderPath.isEmpty()) {
			return chrootRelativePath;
		}

		// Ensure the path starts with /
		if (!chrootRelativePath.startsWith("/")) {
			chrootRelativePath = "/" + chrootRelativePath;
		}

		// Remove duplicate slashes and normalize
		String systemPath = this.chrootFolderPath + chrootRelativePath;
		systemPath = systemPath.replaceAll("/+", "/"); // Replace multiple slashes with single slash
		return Utility.normalizePath(systemPath);
	}

	/**
	 * Convert a system path back to chroot-relative path
	 * 
	 * @param systemPath Full system path
	 * @return Path as it appears inside chroot
	 */
	private String systemToChrootPath(String systemPath) {
		if (this.chrootFolderPath == null || this.chrootFolderPath.isEmpty()) {
			return systemPath;
		}

		if (systemPath.startsWith(this.chrootFolderPath)) {
			String chrootPath = systemPath.substring(this.chrootFolderPath.length());
			return chrootPath.isEmpty() ? "/" : chrootPath;
		}

		return systemPath;
	}

	/**
	 * Check if a directory exists, accounting for chroot
	 *
	 * @param chrootRelativePath Path relative to chroot
	 * @return true if directory exists
	 */
	private boolean directoryExists(String chrootRelativePath) {
		String systemPath = chrootToSystemPath(chrootRelativePath);
		File dir = new File(systemPath);
		boolean exists = dir.exists() && dir.isDirectory();

		classLogger.debug("Directory check - chroot path: '" + chrootRelativePath + "', system path: '" + systemPath
				+ "', exists: " + exists);

		return exists;
	}

	/**
	 * Returns true when {@code candidateChrootPath} resolves (through symlinks)
	 * to a real path outside the confinement root. {@link Utility#normalizePath}
	 * is lexical-only, so a symlink inside the room pointing outward would pass
	 * the existing {@code startsWith} checks; this helper closes that gap.
	 *
	 * <p>Fails closed on any I/O error (path missing, permission denied) — the
	 * caller should already have validated existence via {@link #directoryExists}.
	 */
	private boolean escapesConfinement(String candidateChrootPath) {
		if (this.confinementRoot == null) {
			return false;
		}
		try {
			Path targetReal = Paths.get(chrootToSystemPath(candidateChrootPath)).toRealPath();
			Path rootReal = Paths.get(chrootToSystemPath(this.confinementRoot)).toRealPath();
			boolean escapes = !targetReal.startsWith(rootReal);
			if (escapes) {
				classLogger.warn("Confinement escape blocked: '" + candidateChrootPath
						+ "' resolves to '" + targetReal + "' (root real path: '" + rootReal + "')");
			}
			return escapes;
		} catch (IOException e) {
			classLogger.warn("toRealPath failed for '" + candidateChrootPath
					+ "'; treating as confinement escape: " + e.getMessage());
			return true;
		}
	}

	/**
	 * Execute command with proper path handling
	 */
	public String executeCommand(String command) {
		String output = null;
		try {
			if (command.equalsIgnoreCase("reset")) {
				// Clamp to confinement root when in a room session, not the raw context dir
				this.workingDir = (this.confinementRoot != null) ? this.confinementRoot : this.contextDir;
				output = this.workingDir;
			} else if (command.startsWith("cd")) {
				// remove the cd and then add to working dir
				String originalCommand = command;
				command = command.replace("cd", "");
				command = command.trim();
				classLogger.debug("CD command processing: original='" + originalCommand + "', after cd removal='"
						+ command + "'");

				// Remove quotes if present
				command = removeQuotes(command);
				classLogger.debug("CD command after quote removal: '" + command + "'");

				output = adjustWorkingDir(command);
			} else if (command.startsWith("pwd")) {
				output = this.workingDir;
			} else {
				// this is where we allow other commands
				String[] foutput = runCommand(command);
				String success = foutput[0];
				output = foutput[1];

				// If command succeeded but has no output, that's normal for many commands
				// (mkdir, rm, etc.)
				if (Boolean.parseBoolean(success) && (output == null || output.trim().isEmpty())) {
					output = "Command executed successfully";
				} else if (!Boolean.parseBoolean(success) && (output == null || output.trim().isEmpty())) {
					output = "Command failed with no output";
				}

				// Clean up output formatting if we have output
				if (output != null) {
					output = output.replace("\\", "/");
					output = output.replace("\\r", "");
					output = output.replace("\\n", "");
					output = output.trim();
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return output;
	}

	private void getCommandAppender() {
		String osName = System.getProperty("os.name").toLowerCase();
		if (osName.indexOf("win") >= 0) {
			String terminalMode = Utility.getDefaultTerminalMode();
			if (terminalMode.equals("cmd")) {
				this.commandAppender = "cmd";
			} else {
				// if not cmd, then we are powershell
				this.commandAppender = "powershell.exe";
			}
		} else {
			this.commandAppender = "/bin/bash";
		}
	}

	private String removeQuotes(String input) {
		if (input == null || input.length() < 2) {
			return input;
		}

		String result = input.trim();

		// Keep removing outer quotes until no more are found
		boolean quotesRemoved;
		do {
			quotesRemoved = false;
			// Check for single quotes
			if (result.length() >= 2 && result.startsWith("'") && result.endsWith("'")) {
				result = result.substring(1, result.length() - 1);
				quotesRemoved = true;
			}
			// Check for double quotes
			if (result.length() >= 2 && result.startsWith("\"") && result.endsWith("\"")) {
				result = result.substring(1, result.length() - 1);
				quotesRemoved = true;
			}
		} while (quotesRemoved);

		return result;
	}

	/**
	 * Fixed runCommand method that handles chroot commands properly
	 */
	private String[] runCommand(String command) {
		// Layer 2: if a sandbox policy was attached to this session, the command MUST
		// run through Landlock. Fail closed if the launcher reports unavailable —
		// silently falling through to unsandboxed execution would defeat the policy.
		if (this.sandboxPolicy != null) {
			String[] sandboxed = CmdSandboxLauncher.execute(
					this.sandboxPolicy, this.workingDir, command, this.chrootFolderPath);
			if (sandboxed != null) {
				return sandboxed;
			}
			classLogger.error("Sandbox policy active but Landlock backend unavailable at runtime; "
					+ "refusing command instead of degrading to unsandboxed execution");
			return new String[] { "false",
					"Layer-2 sandbox unavailable on this host; command refused. "
							+ "Either upgrade the kernel to support Landlock or set AGENT_SANDBOX_ENABLE=false." };
		}

		Map<String, String> environment = null;
		String[] foutput = new String[2];
		boolean success = true;

//	    // Prepend safe.directory if this is a git command
//	    // This is because the clone sets the directory ownership as root
//	    // Even when we run with --userspec
//	    if (command.startsWith("git")) {
//	        command = command.replaceFirst("git", "git -c safe.directory='" + this.workingDir+"'");
//	    }

		org.apache.commons.exec.ExecuteWatchdog.Builder executeWatchdogBuilder = ExecuteWatchdog.builder();
		executeWatchdogBuilder.setTimeout(Duration.ofSeconds(20));

		org.apache.commons.exec.DefaultExecutor.Builder<?> executorBuilder = DefaultExecutor.builder();

		// For chroot, we need to set working directory to the chroot base, not the
		// target directory
		File workingDirectory;
		if (this.chrootFolderPath != null && !this.chrootFolderPath.isEmpty()) {
			workingDirectory = new File(this.chrootFolderPath);
		} else {
			workingDirectory = new File(Utility.normalizePath(workingDir));
		}
		executorBuilder.setWorkingDirectory(workingDirectory);

		CommandLine cmdLine = null;

		// Check if we need to use chroot
		if (this.chrootFolderPath != null && !this.chrootFolderPath.isEmpty()) {
			environment = new HashMap<>();
			environment.put("HOME", "/home/default");
			// Validate chroot setup
			File chrootDir = new File(this.chrootFolderPath);
			if (!chrootDir.exists()) {
				classLogger.error("Chroot directory does not exist: " + this.chrootFolderPath);
				foutput[0] = "false";
				foutput[1] = "Chroot directory does not exist: " + this.chrootFolderPath;
				return foutput;
			}

			File bashInChroot = new File(chrootDir, "bin/bash");
			if (!bashInChroot.exists()) {
				classLogger.error("Bash does not exist in chroot: " + bashInChroot.getAbsolutePath());
				foutput[0] = "false";
				foutput[1] = "Bash does not exist in chroot: " + bashInChroot.getAbsolutePath();
				return foutput;
			}

			// Build the complete command as a single string argument
			String escapedWorkingDir = workingDir.replace("'", "'\"'\"'"); // Proper shell escaping
			cmdLine = new CommandLine("fakechroot");
			cmdLine.addArgument("fakeroot");
			cmdLine.addArgument("chroot");
			cmdLine.addArgument("--userspec=1001:1001");
			cmdLine.addArgument(this.chrootFolderPath);
			cmdLine.addArgument(commandAppender);
			cmdLine.addArgument("-c");
			cmdLine.addArgument("cd '" + escapedWorkingDir + "' && " + command, false);
		} else {
			// non-chroot execution
			cmdLine = new CommandLine(commandAppender);
			if (commandAppender.equalsIgnoreCase("/bin/bash")) {
				cmdLine.addArgument("-c");
			} else {
				cmdLine.addArgument("/C");
			}
			cmdLine.addArgument(command, false);
		}

		classLogger.info("Running command executable: " + cmdLine.getExecutable());
		classLogger.info("Complete command: " + cmdLine);

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

			// Log the output for debugging
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

			foutput[0] = String.valueOf(success);
			foutput[1] = output;

		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			success = false;
			foutput[0] = String.valueOf(success);
			foutput[1] = "IO Exception occurred: " + e.getMessage();
		}

		return foutput;
	}

	/**
	 * Adjust working directory with chroot awareness
	 */
	private String adjustWorkingDir(String command) {
		String currentWorkingDir = this.workingDir;

		classLogger.debug("Adjusting working dir - current: '" + currentWorkingDir + "', command: '" + command
				+ "', chroot: '" + this.chrootFolderPath + "'");

		// Handle absolute paths
		if (command.startsWith("/")) {
			// Layer 1: block navigation outside the confinement root
			if (this.confinementRoot != null
					&& !Utility.normalizePath(command).startsWith(Utility.normalizePath(this.confinementRoot))) {
				this.workingDir = currentWorkingDir;
				return "Access denied: path is outside the session root: " + this.confinementRoot;
			}
			// For chroot, we need to check if the path exists within the chroot
			if (directoryExists(command)) {
				// Symlink-aware Layer 1 check: reject targets whose real path is outside
				// the confinement root (lexical startsWith above can't see through symlinks).
				if (escapesConfinement(command)) {
					this.workingDir = currentWorkingDir;
					return "Access denied: target resolves outside the session root: "
							+ this.confinementRoot;
				}
				this.workingDir = command;
				classLogger.debug("Set working dir to absolute path: " + this.workingDir);
				return this.workingDir;
			} else {
				this.workingDir = currentWorkingDir;
				return "Directory doesn't exist: " + command;
			}
		}

		// Handle relative paths
		String[] cdTokens = command.split("/");
		for (int tokenIndex = 0; tokenIndex < cdTokens.length; tokenIndex++) {
			String curToken = cdTokens[tokenIndex];

			// Skip empty tokens (from double slashes)
			if (curToken.isEmpty()) {
				continue;
			}

			if (curToken.equalsIgnoreCase("..")) {
				String[] workdirTokens = this.workingDir.split("/");
				// take out the last one
				int wdTokenLength = workdirTokens.length;
				if (wdTokenLength > 1) {
					String lastToken = workdirTokens[workdirTokens.length - 1];
					int lastIndex = workingDir.lastIndexOf("/" + lastToken);

					String newDir;
					if (lastIndex == 0 || workingDir.equals("/")) {
						// we are at or going to the root
						newDir = "/";
					} else {
						newDir = Utility.normalizePath(workingDir.substring(0, lastIndex));
					}

					// Layer 1: block navigation above the confinement root
					if (this.confinementRoot != null
							&& !Utility.normalizePath(newDir).startsWith(
									Utility.normalizePath(this.confinementRoot))) {
						classLogger.debug("cd .. blocked by confinement root: " + this.confinementRoot);
						return "Cannot navigate above the session root: " + this.confinementRoot;
					}

					if (directoryExists(newDir)) {
						this.workingDir = newDir;
						classLogger.debug("Moved up to directory: " + this.workingDir);
					} else {
						classLogger.debug("Cannot move up - directory doesn't exist: " + newDir);
					}
				} else {
					this.workingDir = currentWorkingDir;
					return "Directory levels doesn't match navigation. Cannot go up from root.";
				}
			} else if (curToken.equals(".")) {
				// Current directory, do nothing
				continue;
			} else {
				String newDir = null;
				if (!this.workingDir.endsWith("/")) {
					newDir = this.workingDir + "/" + curToken;
				} else {
					newDir = this.workingDir + curToken;
				}

				// Normalize the path
				newDir = Utility.normalizePath(newDir);

				// check to see if this is valid using chroot-aware directory check
				if (directoryExists(newDir)) {
					this.workingDir = newDir;
					classLogger.debug("Changed to directory: " + this.workingDir);
				} else {
					this.workingDir = currentWorkingDir;
					return "Directory doesn't exist: " + newDir;
				}
			}
		}

		// Symlink-aware Layer 1 check on the final state: blocks `cd link` where
		// `link` lives inside the room but resolves outside it.
		if (escapesConfinement(this.workingDir)) {
			this.workingDir = currentWorkingDir;
			return "Access denied: target resolves outside the session root: "
					+ this.confinementRoot;
		}

		classLogger.debug("Final working directory: " + this.workingDir);
		return this.workingDir;
	}

	public String getWorkingDir() {
		return this.workingDir;
	}

	public void setWorkingDir(String workingDir) {
		this.workingDir = workingDir;
	}

	public void setConfinementRoot(String confinementRoot) {
		this.confinementRoot = confinementRoot;
	}

	public String getConfinementRoot() {
		return this.confinementRoot;
	}

	public void setSandboxPolicy(SandboxPolicy policy) {
		this.sandboxPolicy = policy;
	}

	/**
	 * Get the actual system path for the current working directory (useful for
	 * debugging)
	 */
	public String getSystemWorkingDir() {
		return chrootToSystemPath(this.workingDir);
	}

	/**
	 * Check if chroot is enabled
	 */
	public boolean isChrootEnabled() {
		return this.chrootFolderPath != null && !this.chrootFolderPath.isEmpty();
	}

	/**
	 * Get the chroot base path
	 */
	public String getChrootPath() {
		return this.chrootFolderPath;
	}

	// keeping example of payload struct format for sending to py socket
	// but will stop using python for this moving forward

//	/**
//	 * 
//	 */
//	public void pushMountToSocket() {
//		PayloadStruct ps = new PayloadStruct();
//		ps.operation = ps.operation.CMD;
//		ps.payload = new Object[] {mountName, mountDir};
//		ps.methodName = "constructor";
//		ps.hasReturn = false;
//		ps.insightId = mountName + "__" + mountDir;
//		PayloadStruct retPS = (PayloadStruct) tcpClient.executeCommand(ps);	
//	}	

}
