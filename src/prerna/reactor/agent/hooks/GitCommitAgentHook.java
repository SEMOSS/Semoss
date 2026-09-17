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

import java.util.Map;
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
	
    @Override
    public void afterRun(AgentRunContext ctx, AgentHarnessResult result) {
        AgentRunTarget target = ctx.getAgentTarget();
        String commitTarget = target != null ? target.toString() : null;
        try {
            if (target != null) {
                if (target.isInsight()) {
                    classLogger.info("GitCommitAgentHook: insight/room target has no project repository; skipping git commit");
                    return;
                }
                String gitFolder = target.getGitFolder();
                String projectId = target.getProjectId();
                User user = ctx.getInsight().getUser();
                GitRepoUtils.addAllChangesAndCommit(gitFolder, true, "Coding Agent Edit", user);
                classLogger.info("GitCommitAgentHook: committed target projectId={}", projectId);
                return;
            }

            Map<String, Object> paramMap = ctx.getAgentConfig().getModelParams();
            String projectId = Objects.toString(paramMap.get("project"), null);
            commitTarget = "projectId=" + projectId;
            if (projectId == null || projectId.trim().isEmpty()) {
                classLogger.error("GitCommitAgentHook: missing project id skipping git commit");
                return;
            }
            IEngine projectEngine = Utility.getProject(projectId.trim());
            if (projectEngine == null) {
                classLogger.error("GitCommitAgentHook: project not found for id={} skipping git commit", projectId);
                return;
            }
            String projectName = projectEngine.getEngineName();
            String gitFolder = EngineUtility.getSpecificEngineVersionFolder(CATALOG_TYPE.PROJECT, projectId.trim(),
                    projectName);
            User user = ctx.getInsight().getUser();
            GitRepoUtils.addAllChangesAndCommit(gitFolder, true, "Coding Agent Edit", user);
        } catch (Exception e) {
            classLogger.error("GitCommitAgentHook: git commit failed for target={}", commitTarget, e);
        }
    }
}
