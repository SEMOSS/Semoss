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
package prerna.reactor.agent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.reactor.agent.hooks.GitCommitAgentHook;

/**
 * Abstract base class for harnesses that build inside a project's {@code client/}
 * folder using the {@code .claude/} convention.
 *
 * <p>Centralises two concerns that {@code ClaudeCodeAgentHarness},
 * {@code GitHubCopilotAgentHarness}, and {@code GitHubCopilotPyAgentHarness} all
 * needed independently:
 * <ul>
 *   <li>Project scaffolding ({@code .claude/skills}, {@code .claude/logs},
 *       {@code CLAUDE.md}) — created on every {@link #execute(AgentRunContext)}.
 *   <li>Skill / {@code CLAUDE.md} CRUD — exposed as static methods so the Pixel
 *       reactors can use them without instantiating a harness.
 * </ul>
 *
 * <p>{@link #execute(AgentRunContext)} is final; subclasses implement
 * {@link #doExecute(AgentRunContext)} instead.
 */
public abstract class AppBuildingHarness implements IAgentHarness {

    private static final Logger logger = LogManager.getLogger(AppBuildingHarness.class);

    protected static final String PARAM_ALLOWED_TOOLS  = "allowed_tools";
    protected static final String PARAM_PERMISSION_MODE = "permission_mode";

    private static final String CLIENT_DIR   = "client";
    static final String CLAUDE_DIR   = ".claude";
    static final String SKILLS_DIR   = "skills";
    private static final String LOGS_DIR     = "logs";
    private static final String CHANGE_LOG   = "change_log.txt";
    private static final String CLAUDE_MD    = "CLAUDE.md";
    static final String SKILL_FILE   = "SKILL.md";

    // ============================================================
    // Template method
    // ============================================================

    @Override
    public final AgentHarnessResult execute(AgentRunContext ctx) throws Exception {
        String clientPath = resolveClientPath(ctx);
        if (clientPath != null) {
            ensureClaudeStructure(clientPath);
            AppBuilderHarnessConfiguration.ensureAgentConfig(clientPath);
        }
        List<IMessageHook> hooks = getMessageHooks();
        for (IMessageHook h : hooks) {
            h.beforeMessage(ctx);
        }
        AgentHarnessResult result = doExecute(ctx);
        for (IMessageHook h : hooks) {
            h.afterMessage(ctx, result);
        }
        return result;
    }

    /** Subclass entry point. Wrapped by {@link #execute(AgentRunContext)}. */
    protected abstract AgentHarnessResult doExecute(AgentRunContext ctx) throws Exception;

    /**
     * Hooks shared by every {@link AppBuildingHarness} subclass. Populate this
     * list with hooks that should run for all harnesses by default. The base
     * {@link #getMessageHooks()} returns this list as-is; subclasses that
     * override {@code getMessageHooks()} are responsible for calling
     * {@code super.getMessageHooks()} to include them (or deliberately omitting
     * the call to opt out).
     */
    private static final List<IMessageHook> COMMON_HOOKS =
            Collections.singletonList(new GitCommitAgentHook());

    /**
     * Hooks fired around each {@link #execute(AgentRunContext)} call, in list
     * order. Default: returns {@link #COMMON_HOOKS}. Subclasses may override
     * to compose, reorder, or replace the common hooks — typically by calling
     * {@code super.getMessageHooks()} and appending their own.
     */
    protected List<IMessageHook> getMessageHooks() {
        return COMMON_HOOKS;
    }

    // ============================================================
    // Shared resolution helpers (used inside doExecute)
    // ============================================================

    protected String resolveClientPath(AgentRunContext ctx) {
        String fp = ctx.getFilePath();
        if (fp == null || fp.trim().isEmpty()) {
            return null;
        }
        return fp + "/" + CLIENT_DIR;
    }

    protected String resolveEngineId(Room room) {
        String engineId = room.getModelId();
        if (engineId == null || engineId.trim().isEmpty()) {
            throw new IllegalArgumentException(getName() + ": room does not have a modelId set");
        }
        return engineId;
    }

    protected String resolveSystemPrompt(Room room) {
        String s = room.getEffectiveSystemPrompt();
        return s == null ? "" : s;
    }

    protected User resolveUser(Insight insight) {
        User user = insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException(getName() + ": insight has no user");
        }
        return user;
    }

    @SuppressWarnings("unchecked")
    protected List<String> resolveAllowedTools(Map<String, Object> params, List<String> defaults) {
        Object o = params.get(PARAM_ALLOWED_TOOLS);
        if (o instanceof List) {
            return (List<String>) o;
        }
        return defaults;
    }

    protected String resolvePermissionMode(Map<String, Object> params) {
        return params.containsKey(PARAM_PERMISSION_MODE)
                ? String.valueOf(params.get(PARAM_PERMISSION_MODE))
                : "default";
    }

    @SuppressWarnings("unchecked")
    protected List<Map<String, String>> buildMcpList(Room room) {
        List<Map<String, String>> result = new ArrayList<>();
        Map<String, Object> opts = room.getOptionsMap();
        if (opts == null || !opts.containsKey("mcp")) {
            return result;
        }
        Object mcpObj = opts.get("mcp");
        if (!(mcpObj instanceof List)) {
            return result;
        }
        for (Object item : (List<?>) mcpObj) {
            if (!(item instanceof Map)) continue;
            Map<String, Object> mcpEntry = (Map<String, Object>) item;
            String id   = mcpEntry.containsKey("id")   ? String.valueOf(mcpEntry.get("id"))   : null;
            String name = mcpEntry.containsKey("name") ? String.valueOf(mcpEntry.get("name")) : id;
            if (id != null) {
                Map<String, String> entry = new HashMap<>();
                entry.put("id",   id);
                entry.put("name", name != null ? name : id);
                result.add(entry);
            }
        }
        return result;
    }

    // ============================================================
    // Project scaffolding (.claude/, CLAUDE.md, logs)
    // ============================================================

    /**
     * Creates {@code .claude/skills}, {@code .claude/logs/change_log.txt},
     * and {@code CLAUDE.md} under {@code clientPath}.
     */
    public static void ensureClaudeStructure(String clientPath) {
        try {
            Path claudeDir = Paths.get(clientPath, CLAUDE_DIR);
            if (!Files.exists(claudeDir)) Files.createDirectories(claudeDir);

            Path skillsDir = claudeDir.resolve(SKILLS_DIR);
            if (!Files.exists(skillsDir)) Files.createDirectories(skillsDir);

            Path logsDir = claudeDir.resolve(LOGS_DIR);
            if (!Files.exists(logsDir)) {
                Files.createDirectories(logsDir);
                Path changeLog = logsDir.resolve(CHANGE_LOG);
                if (!Files.exists(changeLog)) Files.createFile(changeLog);
            }

            Path claudeMd = Paths.get(clientPath, CLAUDE_MD);
            if (!Files.exists(claudeMd)) Files.createFile(claudeMd);

        } catch (IOException e) {
            logger.error("Failed to create .claude directory structure at: {}", clientPath, e);
        }
    }

    // ============================================================
    // Skill / CLAUDE.md CRUD (static methods used by reactors)
    // ============================================================

    public static Boolean createSkill(User user, String projectId, String skillName, String skillContent) {
        String clientPath = resolveProjectClientPath(projectId);
        String slugifiedName = slugify(skillName);
        Path skillPath = Paths.get(clientPath, CLAUDE_DIR, SKILLS_DIR, slugifiedName, SKILL_FILE);

        try {
            Files.createDirectories(skillPath.getParent());
            if (!Files.exists(skillPath)) {
                Files.createFile(skillPath);
            }
            Files.write(skillPath, skillContent.getBytes(StandardCharsets.UTF_8));
            return true;
        } catch (IOException e) {
            logger.error("Failed to write skill file: {}", skillPath, e);
            return false;
        }
    }

    public static Boolean updateSkill(User user, String projectId, String skillName, String skillContent) {
        String clientPath = resolveProjectClientPath(projectId);
        Path skillPath = Paths.get(clientPath, CLAUDE_DIR, SKILLS_DIR, skillName, SKILL_FILE);

        try {
            Files.createDirectories(skillPath.getParent());
            Files.write(skillPath, skillContent.getBytes(StandardCharsets.UTF_8));
            return true;
        } catch (IOException e) {
            logger.error("Failed to write skill file: {}", skillPath, e);
            return false;
        }
    }

    public static Boolean deleteSkill(User user, String projectId, String skillName) {
        String clientPath = resolveProjectClientPath(projectId);
        Path skillPath = Paths.get(clientPath, CLAUDE_DIR, SKILLS_DIR, skillName);

        if (!Files.exists(skillPath)) {
            return true;
        }

        try (Stream<Path> walk = Files.walk(skillPath)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    logger.error("Failed to delete path: {}", path, e);
                    throw new UncheckedIOException(e);
                }
            });
            return true;
        } catch (IOException | UncheckedIOException e) {
            logger.error("Failed to delete skill directory: {}", skillPath, e);
            return false;
        }
    }

    public static Map<String, String> getSkills(User user, String projectId) {
        String clientPath = resolveProjectClientPath(projectId);
        Map<String, String> skillsMap = new HashMap<>();

        Path claudeMd = Paths.get(clientPath, CLAUDE_MD);
        if (Files.exists(claudeMd)) {
            try {
                skillsMap.put("CLAUDE.MD", new String(Files.readAllBytes(claudeMd), StandardCharsets.UTF_8));
            } catch (IOException e) {
                logger.error("Failed to read CLAUDE.md", e);
            }
        }

        Path skillsDir = Paths.get(clientPath, CLAUDE_DIR, SKILLS_DIR);
        if (!Files.exists(skillsDir)) {
            return skillsMap;
        }
        try (Stream<Path> dirs = Files.list(skillsDir)) {
            dirs.forEach(dir -> {
                try {
                    Path skillFile = dir.resolve(SKILL_FILE);
                    if (Files.exists(skillFile)) {
                        skillsMap.put(
                                dir.getFileName().toString(),
                                new String(Files.readAllBytes(skillFile), StandardCharsets.UTF_8));
                    }
                } catch (IOException e) {
                    logger.error("Failed to read skill file under {}", dir, e);
                }
            });
        } catch (IOException e) {
            logger.error("Failed to list skills directory: {}", skillsDir, e);
        }
        return skillsMap;
    }

    static String resolveProjectClientPath(String projectId) {
        IProject project = Utility.getProject(projectId);
        if (project == null) {
            throw new IllegalArgumentException("Could not find or load project = " + projectId);
        }
        String projectName = project.getProjectName();
        String projectPath = EngineUtility.getSpecificEngineAssetsFolder(
                project.getCatalogType(), projectId, projectName);
        return Paths.get(projectPath, CLIENT_DIR).toString();
    }

    private static String slugify(String name) {
        return name.toLowerCase().replace(" ", "-");
    }
}
