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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/**
 * Clones portal files from a GitHub subdirectory to a project Usage:
 * GitCloneIntoProjectPortals(project="projectId",
 * repo="https://github.com/org/repo.git", branch="dev",
 * subdirectory="path/to/portals");
 */
public class GitCloneIntoProjectPortalsReactor extends AbstractReactor {

	private static final String REPO = "repo";
	private static final String BRANCH = "branch";
	private static final String SUBDIRECTORY = "subdirectory";

	public GitCloneIntoProjectPortalsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), REPO, BRANCH, SUBDIRECTORY,
				ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		String projectId = keyValue.get(keysToGet[0]);
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to edit assets.");
		}
		IProject project = Utility.getProject(projectId);

		String repo = keyValue.get(keysToGet[1]);

		String branch = null;
		if (keyValue.containsKey(keysToGet[2])) {
			branch = keyValue.get(keysToGet[2]);
		}

		String subdirectory = null;
		if (keyValue.containsKey(keysToGet[3])) {
			subdirectory = keyValue.get(keysToGet[3]);
		}

		String comment = this.keyValue.get(this.keysToGet[4]);
		if (comment == null) {
			comment = "add: replacing current portals folder with " + repo + " on branch "
					+ (branch == null ? "default branch" : branch)
					+ (subdirectory == null ? "" : (" with subdirectory " + subdirectory));
		}

		String versionGitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(),
				project.getProjectId());
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectId);

		try {
			String mountName = Utility.getRandomString(5);
			String tempClonePath = this.insight.getInsightFolder() + "/" + mountName;

			File tempDir = new File(tempClonePath);
			if (tempDir.exists() && tempDir.isDirectory()) {
				FileUtils.deleteDirectory(tempDir);
			}
			try {
				// Clone the repo to temp location
				CmdExecUtil cloneUtil = new CmdExecUtil(this.insight.getUser(), this.insight.getInsightId(),
						this.insight.getInsightFolder());
				String cloneCommand = null;
				if (branch != null) {
					cloneCommand = "git clone -b " + branch + " " + repo + " " + mountName;
				} else {
					cloneCommand = "git clone " + repo + " " + mountName;
				}
				cloneUtil.executeCommand(cloneCommand);

				if (!tempDir.exists()) {
					throw new RuntimeException("Unable to perform git pull on " + repo);
				}
				// Determine source directory
				File sourceDir;
				if (subdirectory != null && !subdirectory.isEmpty()) {
					sourceDir = new File(tempClonePath + File.separator + subdirectory);
				} else {
					sourceDir = tempDir;
				}

				if (!sourceDir.exists()) {
					throw new RuntimeException(
							"Git pull was successful but directory " + subdirectory + " was not found");
				}

				// Copy portal files directly to project's portals folder
				String portalsFolder = projectAssetFolder + File.separator + "portals";
				File portalsFolderFile = new File(portalsFolder);
				if (portalsFolderFile.exists()) {
					FileUtils.deleteDirectory(portalsFolderFile);
				}
				portalsFolderFile.mkdirs();

				FileUtils.copyDirectory(sourceDir, portalsFolderFile);

				// add file to git
				List<String> gitRelativeFilePaths = new ArrayList<>();
				gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + Constants.PORTALS_FOLDER + "/");

				// Get the user's email
				AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
				String email = accessToken.getEmail();
				String author = accessToken.getUsername();

				GitRepoUtils.addSpecificFiles(versionGitFolder, gitRelativeFilePaths);
				// commit it
				GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
				// handle synchronization to the cloud
				ClusterUtil.pushProjectFolder(project, projectAssetFolder);
			} finally {
				// Clean up temp directory
				if (tempDir.exists()) {
					FileUtils.deleteDirectory(tempDir);
				}
			}
		} catch (Exception e) {
			return NounMetadata.getErrorNounMessage("Failed to clone portal: " + e.getMessage());
		}

		return NounMetadata.getSuccessNounMessage("Successfully cloned portals to project");
	}

	@Override
	public String getReactorDescription() {
		return "Clones portal files from a public GitHub subdirectory to an existing project";
	}

	@Override
	public String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "Project id to clone portal files to";
		} else if (key.equals(REPO)) {
			return "The HTTPS URL of the GitHub repository to clone portal files from";
		} else if (key.equals(BRANCH)) {
			return "The branch of the GitHub repository to clone portal files from";
		} else if (key.equals(SUBDIRECTORY)) {
			return "The subdirectory of the GitHub repository to clone portal files from";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		}
		return super.getDescriptionForKey(key);
	}
}
