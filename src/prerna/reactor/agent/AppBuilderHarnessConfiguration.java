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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.project.api.IProject;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * App-builder helpers for {@code .agents/AGENT_CONFIG.json} and app-local skills.
 *
 * <p>This is a static utility used by the SemossWeb app-builder flow, not a
 * runtime harness class.
 */
public class AppBuilderHarnessConfiguration {

    private static final Logger logger = LogManager.getLogger(AppBuilderHarnessConfiguration.class);

    // Path constants
    /** Subdirectory of a project's assets folder where the agent operates. */
    public static final String CLIENT_DIR = "client";
    /** {@code .claude/} directory under {@link #CLIENT_DIR}. */
    public static final String CLAUDE_DIR = ".claude";
    /** {@code skills/} subdirectory under {@link #CLAUDE_DIR}. */
    public static final String SKILLS_DIR = "skills";
    /** Filename inside each skill folder. */
    public static final String SKILL_FILE = "SKILL.md";

    // AGENT_CONFIG.json constants
    public static final String AGENTS_DIR        = ".agents";
    public static final String AGENT_CONFIG_FILE = "AGENT_CONFIG.json";

    public static final String SELECTED_ENGINES  = "selected_engines";
    public static final String MODEL_ENGINES     = "model_engines";
    public static final String VECTOR_ENGINES    = "vector_engines";
    public static final String STORAGE_ENGINES   = "storage_engines";
    public static final String DATABASE_ENGINES  = "database_engines";

    public static final String ENGINE_NAME = "name";
    public static final String ENGINE_ID   = "id";

    private static final String SELECTED_ENGINES_SKILL_NAME = "selected-engines";
    private static final String SELECTED_ENGINES_SKILL_DESCRIPTION =
            "Consult this skill when introducing a new engine call or adding a new LLM() invocation, "
          + "a new database query against a not-yet-used engine, or a new vector store integration. "
          + "Do not consult it for edits to existing engine calls (the engine ID is already in the code; preserve it). "
          + "Do not consult it for unrelated work (UI, styling, non-engine logic).";

    private static final List<String> ENGINE_TYPES = Arrays.asList(
            MODEL_ENGINES, VECTOR_ENGINES, STORAGE_ENGINES, DATABASE_ENGINES);

    private static final List<String[]> ENGINE_SECTIONS = Arrays.asList(
            new String[] { MODEL_ENGINES,    "Model Engines"    },
            new String[] { VECTOR_ENGINES,   "Vector Engines"   },
            new String[] { STORAGE_ENGINES,  "Storage Engines"  },
            new String[] { DATABASE_ENGINES, "Database Engines" });

    private AppBuilderHarnessConfiguration() {}

    // Scaffolding
    /**
     * Creates {@code .agents/AGENT_CONFIG.json} under {@code clientPath} if it
     * does not yet exist, populated with the default empty engine lists.
     */
    public static void ensureAgentConfig(String clientPath) {
        try {
            Path agentsDir = Paths.get(clientPath, AGENTS_DIR);
            if (!Files.exists(agentsDir)) Files.createDirectories(agentsDir);

            Path configFile = agentsDir.resolve(AGENT_CONFIG_FILE);
            if (!Files.exists(configFile)) {
                writeConfig(configFile, buildDefaultConfig());
            }
        } catch (IOException e) {
            logger.error("Failed to create .agents/AGENT_CONFIG.json at: {}", clientPath, e);
        }
    }

    private static JSONObject buildDefaultConfig() {
        // Selected engines are no longer persisted in AGENT_CONFIG.json. The
        // canonical store is the project-dependency table in the security DB
        // (see SetProjectDependenciesReactor + getProjectDependencyDetails).
        // The .claude/skills/selected-engines/SKILL.md file is regenerated
        // from that dep list whenever dependencies change. AGENT_CONFIG.json
        // is reserved for other agent-side config going forward.
        return new JSONObject();
    }

    // Read
    /**
     * Returns the config under {@code clientPath}, creating the file with
     * defaults if it is missing or unreadable.
     */
    public static JSONObject getConfig(String clientPath) {
        ensureAgentConfig(clientPath);
        Path configFile = Paths.get(clientPath, AGENTS_DIR, AGENT_CONFIG_FILE);
        try {
            String content = new String(Files.readAllBytes(configFile), StandardCharsets.UTF_8);
            if (content.trim().isEmpty()) {
                return buildDefaultConfig();
            }
            return new JSONObject(content);
        } catch (IOException e) {
            logger.error("Failed to read AGENT_CONFIG.json at: {}", configFile, e);
            return buildDefaultConfig();
        }
    }

    /** Convenience accessor by {@code projectId}. */
    public static JSONObject getConfigForProject(String projectId) {
        return getConfig(resolveProjectClientPath(projectId));
    }

    // Skill regeneration (driven by project-dependency changes)
    /**
     * Regenerates {@code .claude/skills/selected-engines/SKILL.md} from the
     * project's current dependency list. Call this from any reactor that
     * mutates project dependencies so the agent-facing skill file always
     * reflects the canonical store.
     *
     * <p>{@code dependencyList} entries are expected to carry at minimum
     * {@code engine_id}, {@code engine_type}, and {@code engine_name} keys
     * (the shape returned by
     * {@link prerna.auth.utils.SecurityProjectUtils#getProjectDependencyDetails}).
     * Non-engine dependency types (e.g. {@code PROJECT}) are ignored.
     *
     * <p>Failures are logged, never thrown — a skill-write failure must not
     * break the dependency-write transaction the caller is wrapping.
     */
    public static void regenerateSelectedEnginesSkillFromDependencies(
            String projectId,
            List<Map<String, Object>> dependencyList) {
        int depCount = dependencyList == null ? 0 : dependencyList.size();
        logger.debug("regenerateSelectedEnginesSkillFromDependencies invoked: project={} depCount={}",
                projectId, depCount);

        if (projectId == null || projectId.trim().isEmpty()) {
            logger.debug("  skipping: blank projectId");
            return;
        }

        // Only Agent 47-managed projects carry a `.claude/` directory. For any
        // other project, writing a selected-engines skill file would scatter
        // Claude Code state into projects that have no use for it (and would
        // create a `.claude/` tree from nothing on the first dep write). The
        // skill exists to be consumed by Claude Code; if Claude Code can't run
        // here, there's nothing to consume it. No-op.
        Path clientPath;
        try {
            clientPath = Paths.get(resolveProjectClientPath(projectId));
        } catch (RuntimeException e) {
            logger.error("  failed to resolve client path for project: {}", projectId, e);
            return;
        }
        Path claudePath = clientPath.resolve(CLAUDE_DIR);
        if (!Files.isDirectory(claudePath)) {
            logger.debug("  skipping: no .claude/ at {}", claudePath);
            return;
        }
        logger.debug("  proceeding: clientPath={} claude exists", clientPath);

        Map<String, List<Map<String, String>>> enginesByType = new LinkedHashMap<>();
        for (String type : ENGINE_TYPES) {
            enginesByType.put(type, new ArrayList<>());
        }

        if (dependencyList != null) {
            for (Map<String, Object> dep : dependencyList) {
                if (dep == null) continue;
                Object idObj = dep.get("engine_id");
                Object typeObj = dep.get("engine_type");
                if (idObj == null || typeObj == null) continue;
                String id = String.valueOf(idObj);
                String typeKey = String.valueOf(typeObj).toLowerCase() + "_engines";
                if (!enginesByType.containsKey(typeKey)) continue; // PROJECT or other
                Object nameObj = dep.get("engine_name");
                String name = nameObj != null ? String.valueOf(nameObj) : id;
                Map<String, String> entry = new LinkedHashMap<>();
                entry.put(ENGINE_NAME, name);
                entry.put(ENGINE_ID,   id);
                enginesByType.get(typeKey).add(entry);
            }
        }

        writeSelectedEnginesSkill(clientPath, enginesByType);
    }

    // Internals
    private static void writeConfig(Path configFile, JSONObject root) throws IOException {
        Files.write(configFile, root.toString(4).getBytes(StandardCharsets.UTF_8));
        // Note: AGENT_CONFIG.json writes no longer drive skill regeneration.
        // The selected-engines skill is regenerated whenever project
        // dependencies change (see regenerateSelectedEnginesSkillFromDependencies).
    }

    /**
     * Writes (or overwrites) {@code .claude/skills/selected-engines/SKILL.md}
     * from a map of engineType -> [{name,id},...]. Errors are logged, not thrown.
     */
    private static void writeSelectedEnginesSkill(
            Path clientPath,
            Map<String, List<Map<String, String>>> enginesByType) {
        Path skillDir = clientPath
                .resolve(CLAUDE_DIR)
                .resolve(SKILLS_DIR)
                .resolve(SELECTED_ENGINES_SKILL_NAME);
        Path skillFile = skillDir.resolve(SKILL_FILE);
        try {
            Files.createDirectories(skillDir);
            String content = buildSelectedEnginesSkill(enginesByType);
            Files.write(skillFile, content.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error("Failed to write selected-engines skill at: {}", skillFile, e);
        }
    }

    private static String buildSelectedEnginesSkill(
            Map<String, List<Map<String, String>>> enginesByType) {
        StringBuilder sb = new StringBuilder();
        sb.append("---\n");
        sb.append("name: ").append(SELECTED_ENGINES_SKILL_NAME).append('\n');
        sb.append("description: '").append(SELECTED_ENGINES_SKILL_DESCRIPTION).append("'\n");
        sb.append("---\n\n");
        sb.append("# Selected Engines\n\n");
        sb.append("These are the engines currently selected for this project. ");
        sb.append("When introducing a new engine call, choose one from the matching list ");
        sb.append("and use its exact `id`.\n\n");

        for (String[] section : ENGINE_SECTIONS) {
            appendEngineSection(sb, section[1], enginesByType, section[0]);
        }
        return sb.toString();
    }

    private static void appendEngineSection(
            StringBuilder sb,
            String title,
            Map<String, List<Map<String, String>>> enginesByType,
            String key) {
        sb.append("## ").append(title).append("\n\n");
        List<Map<String, String>> list = enginesByType == null ? null : enginesByType.get(key);
        if (list == null || list.isEmpty()) {
            sb.append("_None selected._\n\n");
            return;
        }
        for (Map<String, String> e : list) {
            if (e == null) continue;
            String id = e.getOrDefault(ENGINE_ID, "");
            String name = e.getOrDefault(ENGINE_NAME, id.isEmpty() ? "(unknown)" : id);
            sb.append("- **").append(name).append("** `").append(id).append("`\n");
        }
        sb.append('\n');
    }


    // Path resolution
    /**
     * Returns {@code <project-assets>/client} for the given project id.
     *
     * @throws IllegalArgumentException if the project cannot be loaded
     */
    public static String resolveProjectClientPath(String projectId) {
        IProject project = Utility.getProject(projectId);
        if (project == null) {
            throw new IllegalArgumentException("Could not find or load project = " + projectId);
        }
        String projectName = project.getProjectName();
        String projectPath = EngineUtility.getSpecificEngineAssetsFolder(
                project.getCatalogType(), projectId, projectName);
        return Paths.get(projectPath, CLIENT_DIR).toString();
    }

}
