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

import java.io.File;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.util.sandbox.ExecResult;
import prerna.util.sandbox.ISandbox;
import prerna.util.sandbox.SandboxFactory;
import prerna.util.sandbox.SandboxSettings;

public class CmdExecUtil {

	private static final Logger classLogger = LogManager.getLogger(CmdExecUtil.class);
	private static final int EXEC_TIMEOUT_SECONDS = 20;

	private String insightId = null;
	private String chrootFolderPath = null;
	private String workingDir = null;
	private String contextDir = null;
	/** Where commands run: the host, or the user's sandbox. */
	private ISandbox sandbox = SandboxFactory.host();

	/**
	 * Constructor
	 */
	public CmdExecUtil(User user, String insightId, String contextDir) {
		if (user != null && SandboxSettings.chrootEnabled()) {
			SymlinkHelper chrootHelper = user.getUserSymlinkHelper();
			this.chrootFolderPath = chrootHelper.getUserChrootFolder();
			this.sandbox = SandboxFactory.of(chrootHelper, user);
		}
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
	 * Execute command with proper path handling
	 */
	public String executeCommand(String command) {
		if (this.sandbox.capabilities().ownsShellState()) {
			// The sandbox is the boundary and tracks the working directory itself, so it gets
			// the whole line, including cd/pwd/reset, rather than the host-path logic below.
			ExecResult result = this.sandbox.exec(this.workingDir, command, EXEC_TIMEOUT_SECONDS);
			if (result.workingDir() != null) {
				this.workingDir = result.workingDir();
			}
			return result.output();
		}
		String output = null;
		try {
			if (command.equalsIgnoreCase("reset")) {
				this.workingDir = this.contextDir;
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
	/** Execute a shell command while preserving success independently of its output. */
	public String[] executeCommandWithStatus(String command) {
		return runCommand(command);
	}

	private String[] runCommand(String command) {
		ExecResult result = this.sandbox.exec(this.workingDir, command, EXEC_TIMEOUT_SECONDS);
		if (result.workingDir() != null) {
			this.workingDir = result.workingDir();
		}
		return new String[] { String.valueOf(result.success()), result.output() };
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
			// For chroot, we need to check if the path exists within the chroot
			if (directoryExists(command)) {
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

		classLogger.debug("Final working directory: " + this.workingDir);
		return this.workingDir;
	}

	public String getWorkingDir() {
		return this.workingDir;
	}

	public void setWorkingDir(String workingDir) {
		this.workingDir = workingDir;
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
