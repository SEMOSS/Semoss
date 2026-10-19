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
package prerna.reactor.agent.hooks;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.api.IEngine;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.AgentRunTarget;
import prerna.reactor.agent.IAgentRunHook;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitWorkingTreeSnapshot;


/**
 * Hook that runs {@code git add . && git commit} on the resolved project or
 * authenticated user-asset git folder after every agent run.
 *
 * <p>Opt in by adding {@code {"kind": "git_commit"}} to a workspace's
 * {@code WORKSPACE.CONFIG_JSON.hooks[]} (e.g. via {@code SetAgentHooks}).
 * The hook commits the target resolved by {@code AgentRunner}. For legacy
 * contexts without target metadata it falls back to the configured model
 * parameter named {@code project}.
 *
 * <p>Authored as a clean component (no inheritance, no statics) - see
 * {@link prerna.reactor.agent.hooks.AgentHookRegistry#GIT_COMMIT} for the
 * registered kind string.
 */
public final class GitCommitAgentHook implements IAgentRunHook {
	
	private static final Logger classLogger = LogManager.getLogger(GitCommitAgentHook.class);
	private String preRunGitFolder;
	private GitWorkingTreeSnapshot preRunSnapshot;

	/**
	 * Returns repositories in the order they must be committed. Some project
	 * workspaces contain an independently cloned {@code assets} repository that
	 * the outer version repository tracks as a gitlink. Commit the nested
	 * repository first so the outer commit can record its new HEAD.
	 */
	static List<String> getGitFoldersInCommitOrder(String gitFolder) {
		List<String> folders = new ArrayList<>(2);
		File assetsFolder = new File(gitFolder, "assets");
		if (new File(assetsFolder, ".git").exists()) {
			folders.add(assetsFolder.getAbsolutePath());
		}
		folders.add(gitFolder);
		return folders;
	}

	private static String resolveGitFolder(AgentRunContext ctx) {
		AgentRunTarget target = ctx.getAgentTarget();
		if (target != null) {
			return target.isInsight() ? null : target.getGitFolder();
		}
		String projectId = Objects.toString(ctx.getAgentConfig().getModelParams().get("project"), null);
		if (projectId == null || projectId.trim().isEmpty()) {
			return null;
		}
		IEngine project = Utility.getProject(projectId.trim());
		return project == null ? null : EngineUtility.getSpecificEngineVersionFolder(CATALOG_TYPE.PROJECT,
				projectId.trim(), project.getEngineName());
	}

	private static void commitProjectRepositories(List<String> folders, User user, String commitMessage) {
		for (String folder : folders) {
			GitRepoUtils.addAllChangesAndCommit(folder, true, commitMessage, user);
		}
	}

	private static void commitChangedRepositories(String gitFolder, User user, AgentRunContext ctx,
			AgentHarnessResult result, GitWorkingTreeSnapshot preRunSnapshot) throws Exception {
		List<String> folders = getGitFoldersInCommitOrder(gitFolder);
		GitWorkingTreeSnapshot afterRunSnapshot = GitWorkingTreeSnapshot.capture(gitFolder, folders);
		if (preRunSnapshot != null && preRunSnapshot.equals(afterRunSnapshot)) {
			classLogger.info("GitCommitAgentHook: working tree unchanged during run; skipping commit");
			return;
		}
		if (!afterRunSnapshot.hasChanges()) {
			classLogger.info("GitCommitAgentHook: no file changes detected; skipping commit metadata generation");
			return;
		}
		String message = AgentCommitMetadataGenerator.generate(ctx, result, afterRunSnapshot.getChangedFiles());
		commitProjectRepositories(folders, user, message);
	}

	@Override
	public void beforeRun(AgentRunContext ctx) {
		try {
			preRunGitFolder = resolveGitFolder(ctx);
			if (preRunGitFolder != null) {
				preRunSnapshot = GitWorkingTreeSnapshot.capture(preRunGitFolder,
						getGitFoldersInCommitOrder(preRunGitFolder));
			}
		} catch (Exception e) {
			preRunSnapshot = null;
			classLogger.warn("GitCommitAgentHook: unable to snapshot pre-run repository state: {}", e.getMessage());
		}
	}
	
	@Override
	public void afterRun(AgentRunContext ctx, AgentHarnessResult result) {
		AgentRunTarget target = ctx.getAgentTarget();
		String commitTarget = target != null ? target.toString()
				: "projectId=" + Objects.toString(ctx.getAgentConfig().getModelParams().get("project"), null);
		try {
			String gitFolder = resolveGitFolder(ctx);
			if (gitFolder == null) {
				classLogger.info("GitCommitAgentHook: target has no project repository; skipping git commit");
				return;
			}
			User user = ctx.getInsight().getUser();
			commitChangedRepositories(gitFolder, user, ctx, result,
					gitFolder.equals(preRunGitFolder) ? preRunSnapshot : null);
			classLogger.info("GitCommitAgentHook: completed commit processing for target={}", commitTarget);
		} catch (Exception e) {
			classLogger.error("GitCommitAgentHook: git commit failed for target={}", commitTarget, e);
		}
    }
}
