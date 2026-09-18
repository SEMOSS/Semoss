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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.json.JSONObject;

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
	private static final String DEFAULT_COMMIT_MESSAGE = "Coding Agent Edit";
	private static final int MAX_COMMIT_TITLE_LENGTH = 120;
	private static final int MAX_COMMIT_DESCRIPTION_LENGTH = 1000;
	private static final int MAX_PROMPT_TEXT_LENGTH = 4000;
	private String preRunGitFolder;
	private String preRunFingerprint;

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

	static String parseGeneratedCommitMessage(String raw, String fallbackInput) {
		try {
			int start = raw == null ? -1 : raw.indexOf('{');
			int end = raw == null ? -1 : raw.lastIndexOf('}');
			if (start < 0 || end <= start) {
				return fallbackCommitMessage(fallbackInput);
			}
			JSONObject json = new JSONObject(raw.substring(start, end + 1));
			String title = normalizeTitle(json.optString("title", null));
			if (title == null) {
				return fallbackCommitMessage(fallbackInput);
			}
			String description = normalizeDescription(json.optString("description", null));
			return description == null ? title : title + "\n\n" + description;
		} catch (Exception e) {
			return fallbackCommitMessage(fallbackInput);
		}
	}

	private static String normalizeTitle(String title) {
		if (title == null) {
			return null;
		}
		String normalized = title.replaceAll("[\\r\\n]+", " ").trim().replaceAll("\\s+", " ");
		if (normalized.isEmpty()) {
			return null;
		}
		return normalized.substring(0, Math.min(normalized.length(), MAX_COMMIT_TITLE_LENGTH));
	}

	private static String normalizeDescription(String description) {
		if (description == null) {
			return null;
		}
		String normalized = description.trim().replaceAll("\\s+", " ");
		if (normalized.isEmpty()) {
			return null;
		}
		return normalized.substring(0, Math.min(normalized.length(), MAX_COMMIT_DESCRIPTION_LENGTH));
	}

	private static String fallbackCommitMessage(String input) {
		String title = normalizeTitle(input);
		return title == null ? DEFAULT_COMMIT_MESSAGE : title;
	}

	private static List<String> getChangedFiles(List<String> folders) throws Exception {
		LinkedHashSet<String> changed = new LinkedHashSet<>();
		for (int i = 0; i < folders.size(); i++) {
			try (Git git = Git.open(new File(folders.get(i)))) {
				Status status = git.status().call();
				String prefix = folders.size() > 1 && i == 0 ? "assets/" : "";
				for (String path : status.getUncommittedChanges()) {
					changed.add(prefix + path);
				}
				for (String path : status.getUntracked()) {
					changed.add(prefix + path);
				}
			}
		}
		return new ArrayList<>(changed);
	}

	private static String workingTreeFingerprint(List<String> folders) throws Exception {
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		for (String folder : folders) {
			List<String> paths = new ArrayList<>();
			try (Git git = Git.open(new File(folder))) {
				Status status = git.status().call();
				paths.addAll(status.getUncommittedChanges());
				paths.addAll(status.getUntracked());
			}
			Collections.sort(paths);
			for (String path : paths) {
				digest.update(folder.getBytes(StandardCharsets.UTF_8));
				digest.update(path.getBytes(StandardCharsets.UTF_8));
				File file = new File(folder, path);
				if (file.isFile()) {
					digest.update(Files.readAllBytes(file.toPath()));
				}
			}
		}
		return java.util.HexFormat.of().formatHex(digest.digest());
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

	private static String generateCommitMessage(AgentRunContext ctx, AgentHarnessResult result,
			List<String> changedFiles) {
		String prompt = """
				Write Git commit metadata for one coding-agent run. Return JSON only with exactly two string fields:
				{"title":"imperative subject, at most 120 characters","description":"brief explanation of what changed and why"}
				The title must describe the implemented file change, not build status, conversation, or tool activity.

				User request:
				%s

				Final agent response:
				%s

				Changed files:
				%s
				""".formatted(truncate(ctx.getInput(), MAX_PROMPT_TEXT_LENGTH),
				truncate(result == null ? null : result.getFinalText(), MAX_PROMPT_TEXT_LENGTH),
				String.join("\n", changedFiles));
		try {
			Map<String, Object> params = new HashMap<>();
			params.put("temperature", 0.1);
			params.put("max_completion_tokens", 300);
			@SuppressWarnings("deprecation")
			Map<String, Object> response = ctx.getModelEngine().ask(prompt, null, ctx.getInsight(), params).toMap();
			return parseGeneratedCommitMessage(Objects.toString(response.get("response"), null), ctx.getInput());
		} catch (Exception e) {
			classLogger.warn("GitCommitAgentHook: commit metadata generation failed; using user input: {}",
					e.getMessage());
			return fallbackCommitMessage(ctx.getInput());
		}
	}

	private static String truncate(String value, int maxLength) {
		if (value == null) {
			return "";
		}
		return value.substring(0, Math.min(value.length(), maxLength));
	}

	private static void commitProjectRepositories(List<String> folders, User user, String commitMessage) {
		for (String folder : folders) {
			GitRepoUtils.addAllChangesAndCommit(folder, true, commitMessage, user);
		}
	}

	private static void commitChangedRepositories(String gitFolder, User user, AgentRunContext ctx,
			AgentHarnessResult result, String preRunFingerprint) throws Exception {
		List<String> folders = getGitFoldersInCommitOrder(gitFolder);
		String afterRunFingerprint = workingTreeFingerprint(folders);
		if (preRunFingerprint != null && preRunFingerprint.equals(afterRunFingerprint)) {
			classLogger.info("GitCommitAgentHook: working tree unchanged during run; skipping commit");
			return;
		}
		List<String> changedFiles = getChangedFiles(folders);
		if (changedFiles.isEmpty()) {
			classLogger.info("GitCommitAgentHook: no file changes detected; skipping commit metadata generation");
			return;
		}
		commitProjectRepositories(folders, user, generateCommitMessage(ctx, result, changedFiles));
	}

	@Override
	public void beforeRun(AgentRunContext ctx) {
		try {
			preRunGitFolder = resolveGitFolder(ctx);
			if (preRunGitFolder != null) {
				preRunFingerprint = workingTreeFingerprint(getGitFoldersInCommitOrder(preRunGitFolder));
			}
		} catch (Exception e) {
			preRunFingerprint = null;
			classLogger.warn("GitCommitAgentHook: unable to snapshot pre-run repository state: {}", e.getMessage());
		}
	}
	
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
	                commitChangedRepositories(gitFolder, user, ctx, result,
	                        gitFolder.equals(preRunGitFolder) ? preRunFingerprint : null);
	                classLogger.info("GitCommitAgentHook: completed commit processing for target projectId={}",
	                        projectId);
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
	            commitChangedRepositories(gitFolder, user, ctx, result,
	                    gitFolder.equals(preRunGitFolder) ? preRunFingerprint : null);
        } catch (Exception e) {
            classLogger.error("GitCommitAgentHook: git commit failed for target={}", commitTarget, e);
        }
    }
}
