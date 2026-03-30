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
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * User-invoked reactor that delegates to {@link RoomMCPUtility} for project
 * creation and MCP JSON generation, then sets insight context and performs
 * a user-attributed commit.
 * <p>
 * The heavy lifting (project creation, SMSS, MCP JSON) now lives in
 * {@link RoomMCPUtility} and runs automatically at boot via
 * {@code SMSSWebWatcher.init()}. This reactor is kept for manual
 * re-invocation if needed.
 */
public class MakeRoomToolsMCPReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(MakeRoomToolsMCPReactor.class);

    public MakeRoomToolsMCPReactor() {
        this.keysToGet = new String[0];
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        User user = this.insight.getUser();
        if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
            throwAnonymousUserError();
        }

        // delegate to the utility (idempotent)
        RoomMCPUtility.initAndPublish();

        // switch context to the project
        if (!this.insight.setContext(RoomMCPUtility.PROJECT_ID)) {
            throw new IllegalArgumentException(
                    "Unable to set context to project " + RoomMCPUtility.PROJECT_ID + ". User may lack view access.");
        }

        IProject project = Utility.getProject(RoomMCPUtility.PROJECT_ID);

        // user-attributed commit
        MCPUtility.addMCPTag(project);

        String versionGitFolder = AssetUtility.getProjectVersionFolder(
                RoomMCPUtility.PROJECT_NAME, RoomMCPUtility.PROJECT_ID);
        List<String> gitRelativeFilePaths = new ArrayList<>();
        gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + "/mcp/pixel_mcp.json");

        AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
        String email = accessToken.getEmail();
        String author = accessToken.getUsername();

        GitRepoUtils.addSpecificFiles(versionGitFolder, gitRelativeFilePaths);
        GitRepoUtils.commitAddedFiles(versionGitFolder, "add: MakeRoomToolsMCP executed", author, email);

        String assetFolder = AssetUtility.getProjectAssetsFolder(
                RoomMCPUtility.PROJECT_NAME, RoomMCPUtility.PROJECT_ID);
        ClusterUtil.pushProjectFolder(project, assetFolder);

        // return the MCP JSON that was written
        String mcpFilePath = assetFolder + "/mcp/pixel_mcp.json";
        try {
            String content = FileUtils.readFileToString(new File(mcpFilePath), StandardCharsets.UTF_8);
            return new NounMetadata(new JSONObject(content), PixelDataType.JSON_OBJECT);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to read pixel_mcp.json after write: " + e.getMessage());
        }
    }

    @Override
    public String getReactorDescription() {
        return "Creates or reuses a dedicated MCP project for room-file tools, sets the context to that project, "
                + "and generates the pixel_mcp.json with all room tool definitions.";
    }
}
