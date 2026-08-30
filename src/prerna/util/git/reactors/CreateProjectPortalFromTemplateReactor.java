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
package prerna.util.git.reactors;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/**
 * Creates a portal index.html file from HTML content (no external dependencies).
 * Usage: CreateProjectPortalFromTemplate(project="projectId", htmlContent="<html>...</html>");
 * 
 * This reactor provides an optimized way to create portals without git cloning,
 * reducing dependencies and improving reliability.
 */
public class CreateProjectPortalFromTemplateReactor extends AbstractReactor {

	private static final String HTML_CONTENT = "htmlContent";

	public CreateProjectPortalFromTemplateReactor() {
		this.keysToGet = new String[] { 
			ReactorKeysEnum.PROJECT.getKey(), 
			HTML_CONTENT,
			ReactorKeysEnum.COMMENT_KEY.getKey() 
		};
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		String projectId = keyValue.get(keysToGet[0]);
		
		// Security check
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to edit assets.");
		}
		
		IProject project = Utility.getProject(projectId);
		String htmlContent = keyValue.get(keysToGet[1]);
		
		String comment = this.keyValue.get(this.keysToGet[2]);
		if (comment == null) {
			comment = "add: creating portal from bundled template";
		}

		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectId);
		String versionGitFolder = AssetUtility.getProjectVersionFolder(
			project.getProjectName(),
			project.getProjectId()
		);

		try {
			// Create portals directory if it doesn't exist
			File portalsDir = new File(projectAssetFolder + File.separator + "portals");
			if (!portalsDir.exists()) {
				portalsDir.mkdirs();
			}

			// Write the HTML content to index.html
			File portalFile = new File(portalsDir, "index.html");
			try (FileWriter writer = new FileWriter(portalFile, StandardCharsets.UTF_8)) {
				writer.write(htmlContent);
			}

			// Git commit the changes
			List<String> gitRelativeFilePaths = new ArrayList<>();
			gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + Constants.PORTALS_FOLDER + "/");

			// Get the user's credentials for git commit
			AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
			String email = accessToken.getEmail();
			String author = accessToken.getUsername();

			GitRepoUtils.addSpecificFiles(versionGitFolder, gitRelativeFilePaths);
			GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
			
			// Handle synchronization to the cloud
			ClusterUtil.pushProjectFolder(project, projectAssetFolder);

			return new NounMetadata(
				"Portal created successfully at " + portalFile.getAbsolutePath(),
				PixelDataType.CONST_STRING
			);

		} catch (IOException e) {
			throw new IllegalArgumentException("Failed to create portal file: " + e.getMessage(), e);
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to commit portal changes: " + e.getMessage(), e);
		}
	}
}
