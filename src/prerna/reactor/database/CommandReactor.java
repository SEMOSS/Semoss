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
package prerna.reactor.database;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.reactor.agent.sandbox.AgentSandboxConfig;
import prerna.reactor.agent.sandbox.CmdSandboxLauncher;
import prerna.reactor.agent.sandbox.EnforcementMode;
import prerna.reactor.agent.sandbox.SandboxLauncherRegistry;
import prerna.reactor.agent.sandbox.SandboxPolicy;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitPushUtils;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.reactors.GitBaseReactor;

public class CommandReactor extends GitBaseReactor {

	private static final Logger classLogger = LogManager.getLogger(CommandReactor.class);

	private static final Set<String> APPROVED_PROD_COMMANDS = new HashSet<String>(
			Arrays.asList("PULL", "CLONE", "RESET", "STATUS"));

	public CommandReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COMMAND.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		/*
		 * Due to security, we are only allowing this when there is chroot
		 */
		if (!Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
			return NounMetadata.getErrorNounMessage(
					"Terminal/Shell operations are not allowed if chroot is not enabled on the instance");
		}

		String disable_terminal = Utility.getDIHelperProperty(Constants.DISABLE_TERMINAL);
		if (disable_terminal != null && !disable_terminal.isEmpty()) {
			if (Boolean.parseBoolean(disable_terminal)) {
				throw new IllegalArgumentException("Terminal/Shell and user code execution has been disabled.");
			}
		}

		String disable_git_terminal = Utility.getDIHelperProperty(Constants.DISABLE_GIT_TERMINAL);
		if (disable_git_terminal != null && !disable_git_terminal.isEmpty()) {
			if (Boolean.parseBoolean(disable_git_terminal)) {
				throw new IllegalArgumentException("Terminal/Shell has been disabled.");
			}
		}

		String gitProvider = Utility.getDIHelperProperty(Constants.GIT_PROVIDER);
		if (gitProvider == null) {
			gitProvider = "";
		}

		organizeKeys();
		String command = this.keyValue.get(keysToGet[0]);
		User user = this.insight.getUser();
		CmdExecUtil cmdUtil = this.insight.getCmdUtil();

		if (cmdUtil == null) {
			return getError("No context is set - please use SetContext(<mount point>) to set context");
		}

		// Room (MCP) sessions start at the chroot root — seed to the room folder and
		// install Layer 1 (cd confinement) + Layer 2 (Landlock, when enabled).
		if (this.insight.getRoomId() != null && "/".equals(cmdUtil.getWorkingDir())) {
			String roomFolder = this.insight.getInsightFolder();
			cmdUtil.setWorkingDir(roomFolder);
			cmdUtil.setConfinementRoot(roomFolder);

			if (AgentSandboxConfig.resolveEnforcement() == EnforcementMode.ENFORCE) {
				if (!SandboxLauncherRegistry.isAvailable()) {
					return NounMetadata.getErrorNounMessage(
							"AGENT_SANDBOX_ENABLE=true but the Landlock backend is unavailable on this host "
									+ "(kernel must support Landlock ABI v1+; Ubuntu 22.04 / RHEL 9 / "
									+ "AL2023 or newer). Upgrade the kernel or set AGENT_SANDBOX_ENABLE=false.");
				}
				SandboxPolicy policy = CmdSandboxLauncher.buildRoomCommandPolicy(
						roomFolder, cmdUtil.getChrootPath());
				cmdUtil.setSandboxPolicy(policy);
			}
		}

		// uncomment this line to see it in action. We want to test it for .. etc.
		// before committing into play.
		// util = null;
		String git = "";
		String gitCommand = null;
		String preCloneMessage = null;
		String postCloneMessage = null;

		StringTokenizer commands = new StringTokenizer(command);
		if (commands.countTokens() >= 2) {
			git = commands.nextToken().trim();
			gitCommand = commands.nextToken().trim();
		}

		////////////////////////////////////////// PRE PROCESSING
		////////////////////////////////////////// //////////////////////////////////////////////

		// process push
		// try to see if this is a push
		if (git != null && git.equalsIgnoreCase("git") && gitCommand != null && gitCommand.equalsIgnoreCase("push")) {
			NounMetadata pushOutput = processPush(command, cmdUtil.getWorkingDir());
			if (pushOutput != null) {
				return pushOutput;
			}
		}

		// pre processing for clone
		Boolean isCloneAllowed = null;
		if (git != null && git.equalsIgnoreCase("git") && gitCommand != null && gitCommand.equalsIgnoreCase("clone")) {
			isCloneAllowed = preProcessClone(command, cmdUtil.getWorkingDir());

			// allow it but basically say we will blow your git folder away
			if (isCloneAllowed != null && !isCloneAllowed) {
				preCloneMessage = "You are cloning into a folder that is already part of git. Tracking at this level will be disabled";
			}
		}

		// pre-processing for cd
		// basically we try to see if the cd dir being done exists
		// if so no issue ootherwise need to gitclone from the repository
		if (git != null && git.equalsIgnoreCase("cd") && gitCommand != null) {
			File file = new File(Utility.normalizePath(cmdUtil.getWorkingDir()) + "/" + gitCommand);
			if (!file.exists()) {
				// clone the git repository
				cloneRepo(gitCommand, cmdUtil.getWorkingDir());
			}
		}

		// pre-process mkdir to say you cannot create folders at main level
		if (git.equalsIgnoreCase("mkdir") && cmdUtil.getWorkingDir().endsWith("app_root")) {
			return NounMetadata.getErrorNounMessage("You cannot make directory in app root folder");
		}

		// pre process commit
		// add user name and email
		if (git != null && git.equalsIgnoreCase("git") && gitCommand.equalsIgnoreCase("commit")) {
			// add the user name
			// git config user.name
			// git confir user.email
			// and user email
			String[] userEmail = user.getUserCredential(AuthProvider.GITHUB);
			if (userEmail[0] == null) {
				// get it from the email
				userEmail[0] = userEmail[1].substring(0, userEmail[1].indexOf("@"));
			}
			cmdUtil.executeCommand("git config user.name " + userEmail[0]);
			cmdUtil.executeCommand("git config user.email " + userEmail[1]);
		}

		// check that it is only git pull or git clone in prod for CFG
		// for this, trusted repo and default branch must be limited
		if (git != null && git.equalsIgnoreCase("git")
				&& gitProvider.equalsIgnoreCase(AuthProvider.GITLAB.toString())) {
			String trustedRepo = Utility.getDIHelperProperty(Constants.GIT_TRUSTED_REPO);
			String defaultBranch = Utility.getDIHelperProperty(Constants.GIT_DEFAULT_BRANCH);

			if (trustedRepo != null && !trustedRepo.isEmpty()) {
				if (defaultBranch != null && !defaultBranch.isEmpty()) {
					if (!APPROVED_PROD_COMMANDS.contains(gitCommand.toUpperCase())) {
						return NounMetadata.getErrorNounMessage(
								"Only git clone, pull, status, reset are allowed in this environment");
					}
				}
			}
		}

		if (git != null && git.equalsIgnoreCase("git") && gitCommand.equalsIgnoreCase("pull")
				&& gitProvider.equalsIgnoreCase(AuthProvider.GITLAB.toString())) {
			String token = getToken();
			return GitPushUtils.pull(cmdUtil.getWorkingDir(), token, AuthProvider.GITLAB);
		}

		if (git != null && git.equalsIgnoreCase("git") && gitCommand.equalsIgnoreCase("checkout")
				&& gitProvider.equalsIgnoreCase(AuthProvider.GITLAB.toString())) {
			String token = getToken();
			String branch = commands.nextToken();

			return GitPushUtils.checkout(cmdUtil.getWorkingDir(), branch, token, AuthProvider.GITLAB);
		}

		if (git != null && git.equalsIgnoreCase("git") && gitCommand.equalsIgnoreCase("clone")
				&& gitProvider.equalsIgnoreCase(AuthProvider.GITLAB.toString())) {
			String token = getToken();
			String repo = commands.nextToken();

			return GitPushUtils.clone(cmdUtil.getWorkingDir(), repo, token, AuthProvider.GITLAB);
		}

		String output = cmdUtil.executeCommand(command);

		//////////////////////////////////////////
		// POST PROCESSING
		//////////////////////////////////////////

		// post processing
		if (git != null && git.equalsIgnoreCase("git") && gitCommand != null && gitCommand.equalsIgnoreCase("clone")) {
			// try to see if this is a clone if so add it to the clone properties
			postProcessClone(command, cmdUtil.getWorkingDir(), isCloneAllowed);
			postCloneMessage = "If this is a java project, please make sure to adjust the target directory (XML Element build/directory) to ${classesDir}";
		}

		if ((command.startsWith("dir") || command.startsWith("ls"))) {
			// try to see if this is a clone if so add it to the clone properties
			String dir = cmdUtil.getWorkingDir();
			// if(gitCommand != null) - this will break for ls -l
			// dir = gitCommand;
			output = postProcessDir(command, dir, output);
		}
		if (preCloneMessage != null) {
			output = preCloneMessage + "\n" + output;
		}

		if (postCloneMessage != null) {
			output = output + "\n" + postCloneMessage;
		}

		return new NounMetadata(output, PixelDataType.CONST_STRING);
	}

	/**
	 * @param command
	 * @param workingDir
	 * @return
	 */
	private NounMetadata processPush(String command, String workingDir) {
		StringTokenizer commands = new StringTokenizer(command);
		if (commands.countTokens() >= 2) {
			String gitCommand = commands.nextToken().trim();
			String push = commands.nextToken().trim();

			if (gitCommand.equalsIgnoreCase("git") && push.equalsIgnoreCase("push")) {

				// TODO Kunal - This is where I can add the limitations on where you can push to
				// check should be if its not master, its probably okay

				String remoteName = "origin";
				if (commands.hasMoreTokens()) {
					remoteName = commands.nextToken();
				}

				String branch = "master";
				if (commands.hasMoreTokens()) {
					branch = commands.nextToken();
				}

				// need to process this further
				// typically git push origin master

				// get the oauth token
				String token = getToken();

				// do a quick check to see if the remote is
				String url = GitRepoUtils.getConfigRemoteURL(workingDir, remoteName);
				// && url.contains("github")
				if (url != null) // need something to say these are Oauth2'able
				{
					GitPushUtils.push(workingDir, remoteName, branch, token);
					return new NounMetadata("Pushing Git", PixelDataType.CONST_STRING, PixelOperationType.HELP);
				}
			}
		}
		return null;
	}

	/**
	 * @param command
	 * @param workingDir
	 * @param cloneAllowed
	 */
	private void postProcessClone(String command, String workingDir, boolean cloneAllowed) {
		StringTokenizer commands = new StringTokenizer(command);
		if (commands.countTokens() >= 2) {
			String gitCommand = commands.nextToken().trim();
			String gitOperation = commands.nextToken().trim();

			if (gitCommand.equalsIgnoreCase("git") && gitOperation.equalsIgnoreCase("clone")) {
				String repoURL = null;
				if (commands.hasMoreTokens()) {
					repoURL = commands.nextToken();
				}

				String dirName = Utility.getInstanceName(repoURL);

				// see if this directory exists in base folder
				String appBaseFolder = Utility.normalizePath(workingDir);
				if (cloneAllowed && appBaseFolder.endsWith("app_root") && new File(appBaseFolder + "/version").exists()
						&& new File(appBaseFolder + "/" + dirName).exists()) {
					// we are in the right location process now
					// add this to the properties
					// get the root
					File repoFile = new File(appBaseFolder + "/version/repoList.txt");
					if (!repoFile.exists()) {
						try {
							repoFile.createNewFile();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
					Properties prop = Utility.loadProperties(repoFile);
					try (FileOutputStream fos = new FileOutputStream(repoFile)) {
						prop.put(dirName, repoURL);
						prop.store(fos, "Updating");

						// need to commit this file
						// cd into the version
						// git add *
						// git commit -m "adding repos"

						this.insight.getCmdUtil().executeCommand("cd version");
						this.insight.getCmdUtil().executeCommand("git add *");
						this.insight.getCmdUtil().executeCommand("git commit -m \"adding repos\" "); // dont know if we
						// need to add
						// the author
						// here else it
						// complains on
						// config
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}

				else if (!cloneAllowed) {
					File gitFolder = new File(
							Utility.normalizePath(workingDir) + File.separator + dirName + File.separator + ".git");
					if (gitFolder.exists()) {
						try {
							FileUtils.deleteDirectory(gitFolder);
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}
		}
	}

	/**
	 * @param command
	 * @param workingDir
	 * @return
	 */
	private Boolean preProcessClone(String command, String workingDir) {
		StringTokenizer commands = new StringTokenizer(command);
		if (commands.countTokens() >= 2) {
			String gitCommand = commands.nextToken().trim();
			String gitOperation = commands.nextToken().trim();
			if (gitCommand.equalsIgnoreCase("git") && gitOperation.equalsIgnoreCase("clone")) {
				// are you part of a version folder ?
				if (workingDir.startsWith(EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.PROJECT))
						&& workingDir.contains("/version")) {
					return false;
				}
				return true;
			}
		}
		return null;
	}

	/**
	 * @param repoName
	 * @param workingDir
	 */
	private void cloneRepo(String repoName, String workingDir) {
		File repoFile = new File(Utility.normalizePath(workingDir) + "/version/repoList.txt");
		if (repoFile.exists()) {
			Properties prop = Utility.loadProperties(repoFile);
			String url = prop.getProperty(repoName);
			insight.getCmdUtil().executeCommand("git clone " + url);
		}
	}

	/**
	 * @param repoName
	 * @param workingDir
	 * @param output
	 * @return
	 */
	private String postProcessDir(String repoName, String workingDir, String output) {
		String newOutput = output;
		File repoFile = new File(Utility.normalizePath(workingDir) + "/version/repoList.txt");
		if (repoFile.exists()) {
			Properties prop = Utility.loadProperties(repoFile);
			String repos = "While the directories are not shown, Following Repos are available:";
			Enumeration<Object> keys = prop.keys();
			while (keys.hasMoreElements()) {
				repos = repos + "   " + keys.nextElement();
			}

			repos = repos + "\n"
					+ "You can cd into any of these dirs and when you do the git clone will be invoked at this level automatically ";
			repos = repos + "\n\n" + "Version is SEMOSS's default git repository.";
			newOutput = newOutput + "\n" + repos;
		}
		return newOutput;
	}

	@Override
	public String getReactorDescription() {
		return "Executes a shell command in the user's chroot-confined session directory. "
				+ "In a room session, the working directory is anchored to the room folder and "
				+ "navigation outside that folder is blocked. When AGENT_SANDBOX_ENABLE=true and the "
				+ "host kernel supports Landlock, the command also runs under kernel-enforced "
				+ "filesystem confinement; otherwise the command is refused.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.COMMAND.getKey().equals(key)) {
			return "The shell command to execute (e.g., 'pwd', 'ls', 'git status'). Resolved "
					+ "relative to the current session working directory; `cd` can move within the "
					+ "session root but cannot escape it.";
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (ReactorKeysEnum.COMMAND.getKey().equals(key)) {
			return MCP_KEY_TYPE.STRING;
		}
		return super.getKeyTypeForMCP(key);
	}

}
