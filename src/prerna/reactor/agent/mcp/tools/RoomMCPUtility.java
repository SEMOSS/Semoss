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
package prerna.reactor.agent.mcp.tools;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.SmssUtilities;
import prerna.project.api.IProject;
import prerna.project.impl.Project;
import prerna.reactor.IReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/**
 * Utility that creates (or reuses) the dedicated Room Tools MCP project,
 * writes the pixel_mcp.json with hard-coded tool definitions, tags and
 * commits the project, and pushes it to the cluster.
 * <p>
 * Designed to run at boot time with no {@code User} or {@code Insight}
 * context. Call {@link #initAndPublish()} after the security database has
 * been loaded and project folders are available.
 */
public final class RoomMCPUtility {

    private static final Logger classLogger = LogManager.getLogger(RoomMCPUtility.class);

    /** Well-known project name used for the Room Tools MCP project. */
    static final String PROJECT_NAME = "Room Tools MCP";

    /**
     * Deterministic project ID derived from the project name so it survives
     * restarts.
     */
    static final String PROJECT_ID = UUID.nameUUIDFromBytes(
            PROJECT_NAME.getBytes(StandardCharsets.UTF_8)).toString();

    private static final List<Class<? extends IReactor>> ROOM_TOOLS = Arrays.asList(
            ListRoomFilesReactor.class,
            ReadRoomFilesReactor.class,
            SearchRoomFilesWithContextReactor.class,
            GetRoomFileTokenStatsReactor.class,
            GetRoomTokenUsageReactor.class,
            ExecuteRoomShellCommandReactor.class);

    private RoomMCPUtility() {
        // utility class
    }

    // ------------------------------------------------------------------ //
    // Public entry point //
    // ------------------------------------------------------------------ //

    /**
     * Ensures the Room Tools MCP project exists on disk and in the security
     * database, writes the MCP JSON, tags, commits, and pushes to the cluster.
     * <p>
     * Safe to call on every boot — skips work that has already been done.
     * Must be called <b>after</b> the security database is loaded.
     */
    public static void initAndPublish() {
        try {
            ensureProjectOnDisk();
            registerWithDIHelper();
            SecurityProjectUtils.addProject(PROJECT_ID, true, null);
            writeMcpJson();

            // tag, commit, and push to cluster
            IProject project = Utility.getProject(PROJECT_ID);
            if (project != null) {
                MCPUtility.addMCPTag(project);

                String versionGitFolder = AssetUtility.getProjectVersionFolder(PROJECT_NAME, PROJECT_ID);
                List<String> gitRelativeFilePaths = new ArrayList<>();
                gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/" + "mcp/pixel_mcp.json");

                GitRepoUtils.addSpecificFiles(versionGitFolder, gitRelativeFilePaths);
                GitRepoUtils.commitAddedFiles(versionGitFolder, "add: Room Tools MCP initialised at boot");

                String assetFolder = AssetUtility.getProjectAssetsFolder(PROJECT_NAME, PROJECT_ID);
                ClusterUtil.pushProjectFolder(project, assetFolder);
            }

            classLogger.info("Room Tools MCP system project initialised (id={})", PROJECT_ID);
        } catch (Exception e) {
            classLogger.error("Failed to initialise Room Tools MCP system project", e);
        }
    }

    // ------------------------------------------------------------------ //
    // Internal helpers //
    // ------------------------------------------------------------------ //

    /**
     * Creates the project folder and SMSS file if they do not already exist.
     */
    private static void ensureProjectOnDisk() throws IOException {
        String projectBaseFolder = EngineUtility.getSpecificEngineBaseFolder(
                prerna.engine.api.IEngine.CATALOG_TYPE.PROJECT, PROJECT_ID, PROJECT_NAME);
        File projectFolder = new File(projectBaseFolder);
        if (!projectFolder.exists()) {
            projectFolder.mkdirs();
        }

        String smssFilePath = EngineUtility.PROJECT_FOLDER + "/"
                + SmssUtilities.getUniqueName(PROJECT_NAME, PROJECT_ID) + ".smss";
        File smssFile = new File(smssFilePath);
        if (!smssFile.exists()) {
            File tempSmss = SmssUtilities.createTemporaryProjectSmss(
                    PROJECT_ID, PROJECT_NAME, IProject.PROJECT_TYPE.CODE,
                    false, null, null, null, null);
            File target = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
            FileUtils.copyFile(tempSmss, target);
            tempSmss.delete();
            classLogger.info("Created SMSS file for Room Tools MCP at {}", target.getAbsolutePath());
        }
    }

    /**
     * Registers the project with DIHelper so that
     * {@link SecurityProjectUtils#addProject} can locate the SMSS file.
     */
    private static void registerWithDIHelper() {
        String smssFilePath = EngineUtility.PROJECT_FOLDER + "/"
                + SmssUtilities.getUniqueName(PROJECT_NAME, PROJECT_ID) + ".smss";

        DIHelper diHelper = DIHelper.getInstance();
        diHelper.setProjectProperty(PROJECT_ID + "_" + Constants.STORE, smssFilePath);

        String projects = (String) diHelper.getProjectProperty(Constants.PROJECTS);
        if (projects == null) {
            projects = "";
        }
        if (!projects.startsWith(PROJECT_ID)
                && !projects.contains(";" + PROJECT_ID + ";")
                && !projects.endsWith(";" + PROJECT_ID)) {
            projects = projects + ";" + PROJECT_ID;
            diHelper.setProjectProperty(Constants.PROJECTS, projects);
        }

        if (!Utility.projectLoaded(PROJECT_ID)) {
            IProject project = new Project();
            try {
                project.open(smssFilePath);
            } catch (Exception e) {
                classLogger.error("Unable to open Room Tools MCP project from SMSS", e);
            }
            diHelper.setProjectProperty(PROJECT_ID, project);
        }
    }

    /**
     * Writes (or overwrites) the {@code pixel_mcp.json} in the project assets
     * folder with the current set of room tool definitions.
     */
    private static void writeMcpJson() throws IOException {
        String projectAssetFolder = AssetUtility.getProjectAssetsFolder(PROJECT_NAME, PROJECT_ID);

        JSONArray toolsArray = new JSONArray();
        for (Class<? extends IReactor> reactorClass : ROOM_TOOLS) {
            IReactor reactor;
            try {
                reactor = reactorClass.getConstructor().newInstance();
            } catch (Exception e) {
                classLogger.error("Could not instantiate {}", reactorClass.getName(), e);
                continue;
            }
            JSONObject tool = reactor.asMcpTool();
            JSONObject meta = tool.optJSONObject("_meta");
            if (meta == null) {
                meta = new JSONObject();
            }
            meta.put(MCPUtility.SMSS_FUNCTION_NAME, tool.getString("name"));
            meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
            meta.put(MCPUtility.SMSS_MCP_UI, new JSONObject());
            tool.put("_meta", meta);
            toolsArray.put(tool);
        }

        JSONObject mcpJson = new JSONObject();
        mcpJson.put("tools", toolsArray);
        JSONObject topMeta = new JSONObject();
        topMeta.put("last_modified_date",
                LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        mcpJson.put("_meta", topMeta);

        String outputFileLoc = projectAssetFolder + "/mcp/pixel_mcp.json";
        File outputFile = new File(outputFileLoc);
        if (!outputFile.getParentFile().exists()) {
            outputFile.getParentFile().mkdirs();
        }
        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write(mcpJson.toString(4));
        }
    }
}
