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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Manages the {@code .agents/AGENT_CONFIG.json} file that lives alongside the
 * {@code .claude/} directory inside a project's {@code client/} folder.
 *
 * <p>The config tracks engines selected for use by the agent, grouped by type:
 * <pre>
 * {
 *   "selected_engines": {
 *     "model_engines":    [ { "name": "...", "id": "..." }, ... ],
 *     "vector_engines":   [ ... ],
 *     "storage_engines":  [ ... ],
 *     "database_engines": [ ... ]
 *   }
 * }
 * </pre>
 *
 * <p>Scaffolding is invoked from {@link AppBuildingHarness#execute} so the file
 * is guaranteed to exist before any harness {@code doExecute} runs.
 */
public class AppBuilderHarnessConfiguration {

    private static final Logger logger = LogManager.getLogger(AppBuilderHarnessConfiguration.class);

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

    // ============================================================
    // Scaffolding
    // ============================================================

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
        JSONObject root = new JSONObject();
        JSONObject engines = new JSONObject();
        for (String type : ENGINE_TYPES) {
            engines.put(type, new JSONArray());
        }
        root.put(SELECTED_ENGINES, engines);
        return root;
    }

    // ============================================================
    // Read
    // ============================================================

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
        return getConfig(AppBuildingHarness.resolveProjectClientPath(projectId));
    }

    /** Returns the engines list for one type as a {@link JSONArray} (never null). */
    public static JSONArray getEngines(String clientPath, String engineType) {
        validateEngineType(engineType);
        JSONObject root = getConfig(clientPath);
        JSONObject engines = root.optJSONObject(SELECTED_ENGINES);
        if (engines == null) return new JSONArray();
        JSONArray list = engines.optJSONArray(engineType);
        return list != null ? list : new JSONArray();
    }

    // ============================================================
    // Mutate
    // ============================================================

    /**
     * Adds (or replaces, by id) an engine entry under {@code engineType}.
     * Returns true on successful write.
     */
    public static boolean addEngine(String clientPath, String engineType, String name, String id) {
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("Engine id is required");
        }
        return updateEngineList(clientPath, engineType, list -> {
            removeById(list, id);
            JSONObject entry = new JSONObject();
            entry.put(ENGINE_NAME, name != null ? name : id);
            entry.put(ENGINE_ID, id);
            list.put(entry);
        });
    }

    /** Convenience: add an engine resolving {@code clientPath} from {@code projectId}. */
    public static boolean addEngineForProject(String projectId, String engineType, String name, String id) {
        return addEngine(AppBuildingHarness.resolveProjectClientPath(projectId), engineType, name, id);
    }

    /**
     * Removes any engine entry whose id matches under {@code engineType}.
     * Returns true on successful write.
     */
    public static boolean removeEngine(String clientPath, String engineType, String id) {
        if (id == null) return false;
        return updateEngineList(clientPath, engineType, list -> removeById(list, id));
    }

    /** Convenience: remove an engine resolving {@code clientPath} from {@code projectId}. */
    public static boolean removeEngineForProject(String projectId, String engineType, String id) {
        return removeEngine(AppBuildingHarness.resolveProjectClientPath(projectId), engineType, id);
    }

    /**
     * Replaces the entire engine list for {@code engineType} with the provided
     * entries. Each entry must contain {@code name} and {@code id}.
     */
    public static boolean setEngines(String clientPath, String engineType, List<Map<String, String>> entries) {
        return updateEngineList(clientPath, engineType, list -> {
            clear(list);
            if (entries == null) return;
            for (Map<String, String> e : entries) {
                if (e == null) continue;
                String id = e.get(ENGINE_ID);
                if (id == null || id.trim().isEmpty()) continue;
                String name = e.get(ENGINE_NAME);
                JSONObject entry = new JSONObject();
                entry.put(ENGINE_NAME, name != null ? name : id);
                entry.put(ENGINE_ID, id);
                list.put(entry);
            }
        });
    }

    /** Convenience: set engines resolving {@code clientPath} from {@code projectId}. */
    public static boolean setEnginesForProject(String projectId, String engineType, List<Map<String, String>> entries) {
        return setEngines(AppBuildingHarness.resolveProjectClientPath(projectId), engineType, entries);
    }

    /**
     * Bulk-replace any subset of the four engine lists in one write. Any argument
     * that is {@code null} leaves that engine type untouched; a non-null list
     * (including an empty list) replaces it.
     */
    public static boolean setSelectedEngines(String clientPath,
                                             List<Map<String, String>> modelEngines,
                                             List<Map<String, String>> vectorEngines,
                                             List<Map<String, String>> storageEngines,
                                             List<Map<String, String>> databaseEngines) {
        Map<String, List<Map<String, String>>> updates = new LinkedHashMap<>();
        if (modelEngines    != null) updates.put(MODEL_ENGINES,    modelEngines);
        if (vectorEngines   != null) updates.put(VECTOR_ENGINES,   vectorEngines);
        if (storageEngines  != null) updates.put(STORAGE_ENGINES,  storageEngines);
        if (databaseEngines != null) updates.put(DATABASE_ENGINES, databaseEngines);
        return setSelectedEngines(clientPath, updates);
    }

    /** Convenience: bulk-replace resolving {@code clientPath} from {@code projectId}. */
    public static boolean setSelectedEnginesForProject(String projectId,
                                                      List<Map<String, String>> modelEngines,
                                                      List<Map<String, String>> vectorEngines,
                                                      List<Map<String, String>> storageEngines,
                                                      List<Map<String, String>> databaseEngines) {
        return setSelectedEngines(
                AppBuildingHarness.resolveProjectClientPath(projectId),
                modelEngines, vectorEngines, storageEngines, databaseEngines);
    }

    /**
     * Generic bulk-replace by type list map. Validates each key and writes once.
     */
    public static boolean setSelectedEngines(String clientPath,
                                             Map<String, List<Map<String, String>>> updatesByType) {
        if (updatesByType == null || updatesByType.isEmpty()) return true;
        for (String type : updatesByType.keySet()) {
            validateEngineType(type);
        }
        ensureAgentConfig(clientPath);
        Path configFile = Paths.get(clientPath, AGENTS_DIR, AGENT_CONFIG_FILE);

        JSONObject root = getConfig(clientPath);
        JSONObject engines = root.optJSONObject(SELECTED_ENGINES);
        if (engines == null) {
            engines = new JSONObject();
            root.put(SELECTED_ENGINES, engines);
        }
        for (Map.Entry<String, List<Map<String, String>>> e : updatesByType.entrySet()) {
            engines.put(e.getKey(), buildEngineArray(e.getValue()));
        }

        try {
            writeConfig(configFile, root);
            return true;
        } catch (IOException ex) {
            logger.error("Failed to write AGENT_CONFIG.json at: {}", configFile, ex);
            return false;
        }
    }

    /**
     * Returns the engine list for {@code engineType} as a plain
     * {@code List<Map<String, Object>>} suitable for returning from a reactor.
     */
    public static List<Map<String, Object>> getEnginesAsList(String clientPath, String engineType) {
        JSONArray arr = getEngines(clientPath, engineType);
        List<Map<String, Object>> out = new ArrayList<>(arr.length());
        for (int i = 0; i < arr.length(); i++) {
            JSONObject entry = arr.optJSONObject(i);
            if (entry == null) continue;
            Map<String, Object> m = new HashMap<>();
            m.put(ENGINE_NAME, entry.optString(ENGINE_NAME, null));
            m.put(ENGINE_ID,   entry.optString(ENGINE_ID,   null));
            out.add(m);
        }
        return out;
    }

    /** Convenience: get one engine list resolving {@code clientPath} from {@code projectId}. */
    public static List<Map<String, Object>> getEnginesForProject(String projectId, String engineType) {
        return getEnginesAsList(AppBuildingHarness.resolveProjectClientPath(projectId), engineType);
    }

    // ============================================================
    // Internals
    // ============================================================

    private static boolean updateEngineList(String clientPath, String engineType, Consumer<JSONArray> mutator) {
        validateEngineType(engineType);
        ensureAgentConfig(clientPath);
        Path configFile = Paths.get(clientPath, AGENTS_DIR, AGENT_CONFIG_FILE);

        JSONObject root = getConfig(clientPath);
        JSONObject engines = root.optJSONObject(SELECTED_ENGINES);
        if (engines == null) {
            engines = new JSONObject();
            root.put(SELECTED_ENGINES, engines);
        }
        JSONArray list = engines.optJSONArray(engineType);
        if (list == null) {
            list = new JSONArray();
            engines.put(engineType, list);
        }
        mutator.accept(list);

        try {
            writeConfig(configFile, root);
            return true;
        } catch (IOException e) {
            logger.error("Failed to write AGENT_CONFIG.json at: {}", configFile, e);
            return false;
        }
    }

    private static void writeConfig(Path configFile, JSONObject root) throws IOException {
        Files.write(configFile, root.toString(4).getBytes(StandardCharsets.UTF_8));
        // configFile lives at <clientPath>/.agents/AGENT_CONFIG.json
        Path clientPath = configFile.getParent().getParent();
        writeSelectedEnginesSkill(clientPath, root);
    }

    /**
     * Writes (or overwrites) {@code .claude/skills/selected-engines/SKILL.md}
     * with the current selected-engines listing. Errors are logged, not thrown,
     * so a skill-write failure never breaks a config write.
     */
    private static void writeSelectedEnginesSkill(Path clientPath, JSONObject config) {
        Path skillDir = clientPath
                .resolve(AppBuildingHarness.CLAUDE_DIR)
                .resolve(AppBuildingHarness.SKILLS_DIR)
                .resolve(SELECTED_ENGINES_SKILL_NAME);
        Path skillFile = skillDir.resolve(AppBuildingHarness.SKILL_FILE);
        try {
            Files.createDirectories(skillDir);
            String content = buildSelectedEnginesSkill(config);
            Files.write(skillFile, content.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error("Failed to write selected-engines skill at: {}", skillFile, e);
        }
    }

    private static String buildSelectedEnginesSkill(JSONObject config) {
        StringBuilder sb = new StringBuilder();
        sb.append("---\n");
        sb.append("name: ").append(SELECTED_ENGINES_SKILL_NAME).append('\n');
        sb.append("description: '").append(SELECTED_ENGINES_SKILL_DESCRIPTION).append("'\n");
        sb.append("---\n\n");
        sb.append("# Selected Engines\n\n");
        sb.append("These are the engines currently selected for this project. ");
        sb.append("When introducing a new engine call, choose one from the matching list ");
        sb.append("and use its exact `id`.\n\n");

        JSONObject engines = config.optJSONObject(SELECTED_ENGINES);
        for (String[] section : ENGINE_SECTIONS) {
            appendEngineSection(sb, section[1], engines, section[0]);
        }
        return sb.toString();
    }

    private static void appendEngineSection(StringBuilder sb, String title, JSONObject engines, String key) {
        sb.append("## ").append(title).append("\n\n");
        JSONArray list = engines == null ? null : engines.optJSONArray(key);
        if (list == null || list.length() == 0) {
            sb.append("_None selected._\n\n");
            return;
        }
        for (int i = 0; i < list.length(); i++) {
            JSONObject e = list.optJSONObject(i);
            if (e == null) continue;
            String id = e.optString(ENGINE_ID, "");
            String name = e.optString(ENGINE_NAME, id.isEmpty() ? "(unknown)" : id);
            sb.append("- **").append(name).append("** `").append(id).append("`\n");
        }
        sb.append('\n');
    }

    private static JSONArray buildEngineArray(List<Map<String, String>> entries) {
        JSONArray arr = new JSONArray();
        if (entries == null) return arr;
        for (Map<String, String> e : entries) {
            if (e == null) continue;
            String id = e.get(ENGINE_ID);
            if (id == null || id.trim().isEmpty()) continue;
            String name = e.get(ENGINE_NAME);
            JSONObject entry = new JSONObject();
            entry.put(ENGINE_NAME, name != null ? name : id);
            entry.put(ENGINE_ID, id);
            arr.put(entry);
        }
        return arr;
    }

    private static void removeById(JSONArray list, String id) {
        for (int i = list.length() - 1; i >= 0; i--) {
            JSONObject e = list.optJSONObject(i);
            if (e != null && id.equals(e.optString(ENGINE_ID, null))) {
                list.remove(i);
            }
        }
    }

    private static void clear(JSONArray list) {
        for (int i = list.length() - 1; i >= 0; i--) {
            list.remove(i);
        }
    }

    private static void validateEngineType(String engineType) {
        if (!ENGINE_TYPES.contains(engineType)) {
            throw new IllegalArgumentException(
                    "Unknown engine type '" + engineType + "'. Expected one of: " + ENGINE_TYPES);
        }
    }
}
