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
package prerna.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.util.git.GitRepoUtils;

public class AssetUtility {

	// TODO: see which parts should be merged with EngineUtility

	private static final Logger classLogger = LogManager.getLogger(AssetUtility.class);

	private static final String DIR_SEPARATOR = "/";

	public static String USER_SPACE_KEY = "USER";
	public static String INSIGHT_SPACE_KEY = "INSIGHT";

	/**
	 * Grab the root folder to work with asset files
	 * 
	 * PROJECT-ID: project/project_folder/app_root USER: user/user_folder/app_root
	 * INSIGHT: project/project_folder/app_root/version/insightID if saved, else its
	 * the temporary insight folder
	 * 
	 * @param in
	 * @param space
	 * @return
	 */
	public static String getRootFolderPath(Insight in, String space, boolean editRequired) {
		String assetFolder = in.getInsightFolder();
		// find out what space the user wants to use to get the base asset path
		if (space != null && !space.isEmpty()) {
			if (USER_SPACE_KEY.equalsIgnoreCase(space)) {
				User user = in.getUser();
				if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
					throw new IllegalArgumentException("Must be logged in to access user specific assets");
				}
				AuthProvider provider = user.getPrimaryLogin();
				String projectId = user.getAssetProjectId(provider);
				String projectName = "Asset";
				assetFolder = getUserAssetAppRootFolder(projectName, projectId);
			} else if (INSIGHT_SPACE_KEY.equalsIgnoreCase(space)) {
				// default
				// but need to perform check
				if (editRequired && in.isSavedInsight()
						&& !SecurityInsightUtils.userCanEditInsight(in.getUser(), in.getProjectId(), in.getRdbmsId())) {
					throw new IllegalArgumentException("User does not have permission for this insight");
				}
			} else {
				// user has passed an id
				String projectId = space;
				// check if the user has permission for the app
				if (editRequired) {
					if (!SecurityProjectUtils.userCanEditProject(in.getUser(), projectId)) {
						throw new IllegalArgumentException("User does not have permission for this project");
					}
				} else {
					// only read access
					if (!SecurityProjectUtils.userCanViewProject(in.getUser(), projectId)) {
						throw new IllegalArgumentException("User does not have permission for this project");
					}
				}
				IProject project = Utility.getProject(projectId);
				String projectName = project.getProjectName();
				// assetFolder = getAppAssetFolder(appName, appId);
				assetFolder = getProjectAppRootFolder(projectName, projectId);
			}
		} else if (in.isSavedInsight() && editRequired) {
			// we are about to send back the insight folder
			// since that is the default
			// FE very rarely sends the INSIGHT_SPACE_KEY
			// and edit is required
			// make sure user has access
			if (!SecurityInsightUtils.userCanEditInsight(in.getUser(), in.getProjectId(), in.getRdbmsId())) {
				throw new IllegalArgumentException("User does not have permission for this insight");
			}
		}
		assetFolder = Utility.normalizePath(assetFolder.replace('\\', '/'));
		return assetFolder.replace("\\", "/");
	}

	/**
	 * 
	 * @param projectId
	 * @return
	 */
	public static String getProjectAssetsFolder(String projectId) {
		IProject project = Utility.getProject(projectId);
		String projectName = project.getProjectName();
		return AssetUtility.getProjectAssetsFolder(projectName, projectId);
	}

	@Deprecated
	/**
	 * Update to AssetUtility.getProjectAssetsFolder(String projectId) method
	 * 
	 * @param projectId
	 * @return
	 */
	public static String getProjectAssetFolder(String projectId) {
		return getProjectAssetsFolder(projectId);
	}

	/**
	 * 
	 * @param projectName
	 * @param projectId
	 * @return
	 */
	public static String getProjectAssetsFolder(String projectName, String projectId) {
		String projectVersionBaseFolder = getProjectVersionFolder(projectName, projectId);
		String projectFolder = projectVersionBaseFolder + DIR_SEPARATOR + Constants.ASSETS_FOLDER;

		// if this folder does not exist create it
		File file = new File(Utility.normalizePath(projectFolder));
		if (!file.exists()) {
			file.mkdir();
		}
		return projectFolder.replace("\\", "/");
	}

	/**
	 * 
	 * @param projectId
	 * @return
	 */
	public static String getProjectPortalsFolder(String projectId) {
		IProject project = Utility.getProject(projectId);
		String projectName = project.getProjectName();
		return AssetUtility.getProjectPortalsFolder(projectName, projectId);
	}

	/**
	 * 
	 * @param projectName
	 * @param projectId
	 * @return
	 */
	public static String getProjectPortalsFolder(String projectName, String projectId) {
		String assetFolder = getProjectAssetsFolder(projectName, projectId);
		String portalsFolder = assetFolder + DIR_SEPARATOR + Constants.PORTALS_FOLDER;

		// if this folder does not exist create it
		File file = new File(portalsFolder);
		if (!file.exists()) {
			file.mkdir();
		}
		return portalsFolder.replace("\\", "/");
	}

	/**
	 * 
	 * @param projectName
	 * @param projectId
	 * @return
	 */
	public static String getProjectNotebookFolder(String projectName, String projectId) {
		String assetFolder = getProjectAssetsFolder(projectName, projectId);
		String notebookFolder = assetFolder + DIR_SEPARATOR + IProject.NOTEBOOK_FOLDER;

		// if this folder does not exist create it
		File file = new File(notebookFolder);
		if (!file.exists()) {
			file.mkdir();
		}
		return notebookFolder.replace("\\", "/");
	}

	/**
	 * 
	 * @param projectName
	 * @param projectId
	 * @return
	 */
	public static String getProjectVersionFolder(String projectName, String projectId) {
		String projectBaseFolder = getProjectAppRootFolder(projectName, projectId);
		String gitFolder = projectBaseFolder + DIR_SEPARATOR + Constants.VERSION_FOLDER;
		// if this folder does not exist create it
		File file = new File(Utility.normalizePath(gitFolder));
		if (!file.exists()) {
			file.mkdir();
		}

		if (!isGit(gitFolder)) {
			GitRepoUtils.init(gitFolder);
		}
		return gitFolder.replace("\\", "/");
	}

	/**
	 * 
	 * @param in
	 * @param space
	 * @return
	 */
	public static String getAssetRelativePath(Insight in, String space) {
		String relativePath = "";
		if (space == null || space.equals(INSIGHT_SPACE_KEY)) {
			relativePath = "/" + Constants.VERSION_FOLDER + "/" + in.getRdbmsId();
		} else {
			// user space or asset app
			// asset app - no relative space ?
			relativePath = "";
			// relativePath = Constants.ASSETS_FOLDER;
		}
		return relativePath.replace("\\", "/");
	}

	/**
	 * 
	 * @param assetFolder
	 * @return
	 */
	public static boolean isGit(String assetFolder) {
		File file = new File(Utility.normalizePath(assetFolder) + DIR_SEPARATOR + ".git");
		return file.exists();
	}

	/**
	 * 
	 * @param projectId
	 * @return
	 */
	public static String getProjectAppRootFolder(String projectId) {
		IProject project = Utility.getProject(projectId);
		String projectName = project.getProjectName();
		return AssetUtility.getProjectAppRootFolder(projectName, projectId);
	}

	/**
	 * 
	 * @param projectName
	 * @param projectId
	 * @return
	 */
	public static String getProjectAppRootFolder(String projectName, String projectId) {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		if (!(baseFolder.endsWith("/") || baseFolder.endsWith("\\"))) {
			baseFolder += DIR_SEPARATOR;
		}

		String baseProjectFolder = Utility.normalizePath(baseFolder + Constants.PROJECT_FOLDER + DIR_SEPARATOR
				+ SmssUtilities.getUniqueName(projectName, projectId) + DIR_SEPARATOR + Constants.APP_ROOT_FOLDER);

		File baseProjectFolderFile = new File(baseProjectFolder);
		if (!baseProjectFolderFile.exists()) {
			baseProjectFolderFile.mkdir();
			// if you are creating this.. there is a possibility we need to fix this project
			rehomeProjectForAppRoot(projectName, projectId, baseProjectFolder);
		}
		// try to see if there is a version folder and if so move it into app_root
		return baseProjectFolder.replace("\\", "/");
	}

	/**
	 * 
	 * @param projectName
	 * @param projectId
	 * @param newRoot
	 */
	private static void rehomeProjectForAppRoot(String projectName, String projectId, String newRoot) {
		String baseFolder = Utility.getBaseFolder();
		if (!(baseFolder.endsWith("/") || baseFolder.endsWith("\\"))) {
			baseFolder += DIR_SEPARATOR;
		}

		String oldBaseAppFolder = Utility.normalizePath(baseFolder + Constants.PROJECT_FOLDER + DIR_SEPARATOR
				+ SmssUtilities.getUniqueName(projectName, projectId) + DIR_SEPARATOR + Constants.VERSION_FOLDER);

		File oldBaseAppFolderFile = new File(oldBaseAppFolder);

		if (oldBaseAppFolderFile.exists()) {
			try {
				classLogger.info("Rehoming project catalog {} - moving {} under {}",
						SmssUtilities.getUniqueName(projectName, projectId), oldBaseAppFolder, newRoot);
				Files.move(oldBaseAppFolderFile.toPath(),
						new File(newRoot + DIR_SEPARATOR + Constants.VERSION_FOLDER).toPath(),
						StandardCopyOption.REPLACE_EXISTING);
			} catch (IOException e) {
				classLogger.error("Failed to rehome the project version folder {} under {}", oldBaseAppFolder, newRoot,
						e);
			}
		}
	}

	/*
	 * USER ASSET METHODS
	 */

	/**
	 * 
	 * @param projectName
	 * @param projectId
	 * @return
	 */
	public static String getUserAssetVersionFolder(String projectName, String projectId) {
		// get the base folder
		String baseFodler = getUserAssetAppRootFolder(projectName, projectId);
		String gitFolder = baseFodler + "/version";

		File file = new File(Utility.normalizePath(gitFolder));
		if (!file.exists() && !file.mkdirs()) {
			classLogger.warn("Unable to create the user asset version folder {}", gitFolder);
		}

		if (!isGit(gitFolder)) {
			GitRepoUtils.init(gitFolder);
		}
		return gitFolder.replace("\\", "/");
	}

	/**
	 * 
	 * @param projectName
	 * @param projectId
	 * @return
	 */
	public static String getUserAssetFolder(String projectName, String projectId) {
		String projectVersionBaseFolder = getUserAssetVersionFolder(projectName, projectId);
		String projectFolder = projectVersionBaseFolder + DIR_SEPARATOR + Constants.ASSETS_FOLDER;

		// if this folder does not exist create it
		File file = new File(Utility.normalizePath(projectFolder));
		if (!file.exists() && !file.mkdirs()) {
			classLogger.warn("Unable to create the user asset folder {}", projectFolder);
		}
		return projectFolder.replace("\\", "/");
	}

	/**
	 * 
	 * @param projectName
	 * @param projectId
	 * @return
	 */
	public static String getUserAssetAppRootFolder(String projectName, String projectId) {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		if (!(baseFolder.endsWith("/") || baseFolder.endsWith("\\"))) {
			baseFolder += DIR_SEPARATOR;
		}

		String baseProjectFolder = Utility.normalizePath(baseFolder + Constants.USER_FOLDER + DIR_SEPARATOR
				+ SmssUtilities.getUniqueName(projectName, projectId) + DIR_SEPARATOR + Constants.APP_ROOT_FOLDER);

		File baseAppFolderFile = new File(baseProjectFolder);
		if (!baseAppFolderFile.exists()) {
			// mkdirs and not mkdir - the user/<asset project> parent will not exist when
			// the asset project is registered in the security db but its folder is not on
			// this instance (fresh volume, restored db, cleared user folder)
			if (!baseAppFolderFile.mkdirs()) {
				classLogger.warn("Unable to create the user asset app root folder {}", baseProjectFolder);
			}
			// if you are creating this.. there is a possibility we need to fix this engine
			rehomeUserForAppRoot(projectName, projectId, baseProjectFolder);
		}
		// try to see if there is a version folder and if so move it into app_root
		return baseProjectFolder.replace("\\", "/");
	}

	/**
	 * 
	 * @param projectName
	 * @param projectId
	 * @param newRoot
	 */
	private static void rehomeUserForAppRoot(String projectName, String projectId, String newRoot) {
		String baseFolder = Utility.getBaseFolder();
		if (!(baseFolder.endsWith("/") || baseFolder.endsWith("\\"))) {
			baseFolder += DIR_SEPARATOR;
		}

		String oldBaseAppFolder = Utility.normalizePath(baseFolder + Constants.USER_FOLDER + DIR_SEPARATOR
				+ SmssUtilities.getUniqueName(projectName, projectId) + DIR_SEPARATOR + Constants.VERSION_FOLDER);

		File oldBaseAppFolderFile = new File(oldBaseAppFolder);

		if (oldBaseAppFolderFile.exists()) {
			try {
				classLogger.info("Rehoming user asset catalog {} - moving {} under {}",
						SmssUtilities.getUniqueName(projectName, projectId), oldBaseAppFolder, newRoot);
				Files.move(oldBaseAppFolderFile.toPath(),
						new File(newRoot + DIR_SEPARATOR + Constants.VERSION_FOLDER).toPath(),
						StandardCopyOption.REPLACE_EXISTING);
			} catch (IOException e) {
				classLogger.error("Failed to rehome the user asset version folder {} under {}", oldBaseAppFolder,
						newRoot, e);
			}
		}
	}

}
