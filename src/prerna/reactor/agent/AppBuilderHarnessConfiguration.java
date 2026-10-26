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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * App-builder helpers for {@code .agents/AGENT_CONFIG.json} and the
 * "Selected Engines" system prompt block.
 *
 * <p>This is a static utility used by the SemossWeb app-builder flow, not a
 * runtime harness class.
 */
public class AppBuilderHarnessConfiguration {

    private static final Logger logger = LogManager.getLogger(AppBuilderHarnessConfiguration.class);

    // Path constants
    /** Subdirectory of a project's assets folder where the agent operates. */
    public static final String CLIENT_DIR = "client";

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

    /**
     * Builds the "Selected Engines" system prompt block for a project. Reads
     * the dependency list fresh from the canonical store
     * ({@link prerna.auth.utils.SecurityProjectUtils#getProjectDependencyDetails})
     * so workbench selections are always current at run time, regardless of
     * which reactor last mutated them.
     *
     * <p>Non-engine dependency types (e.g. {@code PROJECT}) are ignored.
     *
     * @return prompt block, or {@code null} when {@code projectId} is blank or
     *         the dependency lookup fails (logged, never thrown)
     */
    public static String buildSelectedEnginesPrompt(String projectId) {
        if (projectId == null || projectId.trim().isEmpty()) {
            return null;
        }
        List<Map<String, Object>> dependencyList;
        try {
            dependencyList = SecurityProjectUtils.getProjectDependencyDetails(projectId);
        } catch (Exception e) {
            logger.warn("Failed to load dependency details for selected-engines prompt, project: {}",
                    projectId, e);
            return null;
        }
        return buildSelectedEnginesPromptBody(bucketEnginesByType(dependencyList));
    }

    /**
     * Buckets dependency rows into engineTypeKey -> [{name,id},...] for the
     * four prompt-visible engine types.
     */
    private static Map<String, List<Map<String, String>>> bucketEnginesByType(
            List<Map<String, Object>> dependencyList) {
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
        return enginesByType;
    }

    // Internals
    private static void writeConfig(Path configFile, JSONObject root) throws IOException {
        Files.write(configFile, root.toString(4).getBytes(StandardCharsets.UTF_8));
    }

    private static String buildSelectedEnginesPromptBody(
            Map<String, List<Map<String, String>>> enginesByType) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Selected Engines\n\n");
        sb.append("The user pre-selected these engines for this project in the workbench ");
        sb.append("Available Engines panel. When introducing a new engine call (LLM invocation, ");
        sb.append("database query, vector store, storage), choose from the matching list below ");
        sb.append("and use its exact `id`.\n");
        sb.append("If exactly one engine is listed for the type you need, use it without asking. ");
        sb.append("If several are listed, ask the user which of the listed engines to use. ");
        sb.append("Only when the list for that type is empty should you ask the user to choose ");
        sb.append("or attach an engine. Never use an unlisted engine merely because it is ");
        sb.append("accessible to you.\n\n");

        for (String[] section : ENGINE_SECTIONS) {
            appendEngineSection(sb, section[1], enginesByType, section[0]);
        }
        return sb.toString().trim();
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
