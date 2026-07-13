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
package prerna.reactor.project;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.reflect.TypeToken;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.LegacyToProjectRestructurerHelper;
import prerna.engine.impl.SmssUtilities;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.UploadInputUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import prerna.util.gson.GsonUtility;

public class UploadProjectReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(UploadProjectReactor.class);

	private static final String CLASS_NAME = UploadProjectReactor.class.getName();

	public UploadProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.GLOBAL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = this.getLogger(CLASS_NAME);
		int step = 1;
		String zipFilePath = UploadInputUtility.getFilePath(this.store, this.insight);
		// do we want this project to be accessible to everyone
		boolean global = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.GLOBAL.getKey()) + "");
		// check security
		// Need to check this, will the same methods work/enhanced to check the
		// permissions on project?
		User user = this.insight.getUser();
		LegacyToProjectRestructurerHelper legacyToProjectRestructurerHelper = new LegacyToProjectRestructurerHelper();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create or upload a project",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		// throw error is user doesn't have rights to publish new apps
		if (AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(user)) {
			throwUserNotPublisherError();
		}

		if (AbstractSecurityUtils.adminOnlyProjectAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			AbstractReactor.throwFunctionalityOnlyExposedForAdminsError();
		}

		if (global && (AbstractSecurityUtils.adminOnlyProjectSetPublic() && !SecurityAdminUtils.userIsAdmin(user))) {
			SemossPixelException exception = new SemossPixelException(
					NounMetadata.getErrorNounMessage("User can upload a project but cannot make the project public"));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// creating a temp folder to unzip project folder and smss
		String randomIdAsDir = UUID.randomUUID().toString();
		String projectFolderPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR
				+ Constants.PROJECT_FOLDER;
		String randomTempUnzipFolderPath = projectFolderPath + DIR_SEPARATOR + randomIdAsDir;
		File randomTempUnzipF = new File(randomTempUnzipFolderPath);

		// gotta keep track of the smssFile and files unzipped
		Map<String, List<String>> filesAdded = new HashMap<>();
		List<String> fileList = new ArrayList<>();
		String smssFileLoc = null;
		File smssFile = null;
		// unzip files to temp project folder
		boolean error = false;
		try {
			logger.info(step + ") Unzipping project");
			filesAdded = ZipUtils.unzip(zipFilePath, randomTempUnzipFolderPath);
			logger.info(step + ") Done");
			step++;

			// look for smss file
			fileList = filesAdded.get("FILE");
			logger.info(step + ") Searching for smss");
			for (String filePath : fileList) {
				if (!filePath.startsWith("__MACOSX/") && filePath.endsWith(Constants.SEMOSS_EXTENSION)) {
					smssFileLoc = randomTempUnzipFolderPath + DIR_SEPARATOR + filePath;
					smssFile = new File(Utility.normalizePath(smssFileLoc));
					// check if the file exists
					if (!smssFile.exists()) {
						// invalid file need to delete the files unzipped
						smssFileLoc = null;
					}
					break;
				}
			}
			logger.info(step + ") Done");
			step++;

			// delete the files if we were unable to find the smss file
			if (smssFileLoc == null) {
				throw new SemossPixelException("Unable to find " + Constants.SEMOSS_EXTENSION + " file", false);
			}
		} catch (SemossPixelException e) {
			error = true;
			throw e;
		} catch (Exception e) {
			error = true;
			classLogger.error("Error occurred while unzipping the files", e);
			throw new SemossPixelException("Error occurred while unzipping the files", false);
		} finally {
			if (error) {
				cleanUpFolders(randomTempUnzipF);
			}
		}

		String projects = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
		String projectId = null;
		String projectName = null;
		IProject.PROJECT_TYPE projectEnumType = IProject.PROJECT_TYPE.INSIGHTS;
		String projectGitProvider = null;
		String projectGitCloneUrl = null;

		File finalProjectSmssF = null;
		File finalProjectFolderF = null;
		Boolean isLegacy = false;
		boolean projectAddedToDIHelper = false;
		try {
			logger.info(step + ") Reading smss");
			Properties prop = Utility.loadProperties(smssFileLoc);
			if (prop.getProperty(Constants.ENGINE) != null || prop.getProperty(Constants.ENGINE_ALIAS) != null
					|| prop.getProperty(Constants.ENGINE_TYPE) != null) {
				isLegacy = true;
			}

			// pull some properties out for creating an smss if legacy format
			if (isLegacy) {
				projectId = prop.getProperty(Constants.ENGINE);
				projectName = prop.getProperty(Constants.ENGINE_ALIAS);
			} else {
				projectId = prop.getProperty(Constants.PROJECT);
				projectName = prop.getProperty(Constants.PROJECT_ALIAS);
			}
			projectGitProvider = prop.getProperty(Constants.PROJECT_GIT_PROVIDER);
			projectGitCloneUrl = prop.getProperty(Constants.PROJECT_GIT_CLONE);

			// check if project id already exists in security db
			if (SecurityProjectUtils.projectExists(projectId)) {
				cleanUpFolders(randomTempUnzipF);
				SemossPixelException exception = new SemossPixelException(
						NounMetadata.getErrorNounMessage("Project id already exists"));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}

			logger.info(step + ") Done");
			step++;

			// zip file has the smss and project folder on the same level
			// need to move these files around
			String tempUnzippedProjectFolderPath = randomTempUnzipFolderPath + DIR_SEPARATOR
					+ SmssUtilities.getUniqueName(projectName, projectId);
			File tempUnzippedProjectF = new File(Utility.normalizePath(tempUnzippedProjectFolderPath));
			finalProjectFolderF = new File(Utility.normalizePath(
					projectFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(projectName, projectId)));
			finalProjectSmssF = new File(Utility.normalizePath(projectFolderPath + DIR_SEPARATOR
					+ SmssUtilities.getUniqueName(projectName, projectId) + Constants.SEMOSS_EXTENSION));

			// need to ignore file watcher
			if (!(projects.startsWith(projectId) || projects.contains(";" + projectId + ";")
					|| projects.endsWith(";" + projectId))) {
				String newProjects = projects + ";" + projectId;
				DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, newProjects);
				projectAddedToDIHelper = true;
			} else {
				SemossPixelException exception = new SemossPixelException(
						NounMetadata.getErrorNounMessage("Project id already exists"));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}

			if (isLegacy) {
				legacyToProjectRestructurerHelper.userScanAndCopyInsightsDatabaseIntoNewProjectFolder(
						Utility.normalizePath(projectFolderPath + DIR_SEPARATOR
								+ SmssUtilities.getUniqueName(projectName, projectId)),
						Utility.normalizePath(tempUnzippedProjectFolderPath), false);

				legacyToProjectRestructurerHelper.userScanAndCopyVersionsIntoNewProjectFolder(
						Utility.normalizePath(projectFolderPath + DIR_SEPARATOR
								+ SmssUtilities.getUniqueName(projectName, projectId)),
						Utility.normalizePath(tempUnzippedProjectFolderPath), false);

				// move project folder
				logger.info(step + ") Done");
				step++;

				// move smss file
				File tempUnzippedSmssF = SmssUtilities.createTemporaryProjectSmss(projectId, projectName,
						projectEnumType, projectGitProvider, projectGitCloneUrl, null);
				FileUtils.copyFile(tempUnzippedSmssF, finalProjectSmssF);
				tempUnzippedSmssF.delete();
				logger.info(step + ") Done");
				step++;
			} else {
				// move project folder
				logger.info(step + ") Moving project folder");
				FileUtils.copyDirectory(tempUnzippedProjectF, finalProjectFolderF);
				logger.info(step + ") Done");
				step++;
				// move smss file
				logger.info(step + ") Moving smss file");
				File tempUnzippedSmssF = new File(Utility.normalizePath(randomTempUnzipF + DIR_SEPARATOR
						+ SmssUtilities.getUniqueName(projectName, projectId) + Constants.SEMOSS_EXTENSION));
				FileUtils.copyFile(tempUnzippedSmssF, finalProjectSmssF);
				logger.info(step + ") Done");
				step++;
			}

		} catch (Exception e) {
			error = true;
			classLogger.error("Error copying the files over from the temp zip location to the final project folder", e);
			throw new SemossPixelException(e.getMessage(), false);
		} finally {
			if (error) {
				// remove from DIHelper
				if (projectAddedToDIHelper) {
					UploadUtilities.removeProjectFromDIHelper(projectId);
				}
				cleanUpFolders(randomTempUnzipF, finalProjectSmssF, finalProjectFolderF);
			} else {
				// just delete the temp project folder
				cleanUpFolders(randomTempUnzipF);
			}
		}

		try {
			DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE,
					finalProjectSmssF.getAbsolutePath());

			// ensure PROJECT_DISPLAY_NAME is present in the smss file
			// if not present, seed it from PROJECT_ALIAS so the file stays consistent
			Properties smssProps = Utility.loadProperties(finalProjectSmssF.getAbsolutePath());
			String displayName = smssProps.getProperty(Constants.PROJECT_DISPLAY_NAME);
			if (displayName == null || displayName.trim().isEmpty()) {
				try {
					Utility.changePropertiesFileValue(finalProjectSmssF.getAbsolutePath(),
							Constants.PROJECT_DISPLAY_NAME, projectName);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}

			logger.info(step + ") Grabbing project insights");
			SecurityProjectUtils.addProject(projectId, global, user);

			// see if we have any dependencies or metadata to load
			{
				File metadataFile = new File(
						finalProjectFolderF.getAbsolutePath() + "/" + projectName + IEngine.METADATA_FILE_SUFFIX);
				if (metadataFile.exists() && metadataFile.isFile()) {
					Map<String, Object> metadata = (Map<String, Object>) GsonUtility.readJsonFileToObject(metadataFile,
							new TypeToken<Map<String, Object>>() {
							}.getType());
					SecurityProjectUtils.updateProjectMetadata(projectId, metadata);
					// delete this file since values can update and file is dynamically generated on
					// export
					metadataFile.delete();
				}

				File dependenciesFile = new File(
						finalProjectFolderF.getAbsolutePath() + "/" + projectName + IProject.DEPENDENCIES_FILE_SUFFIX);
				if (dependenciesFile.exists() && dependenciesFile.isFile()) {
					List<Map<String, Object>> projectDependencies = (List<Map<String, Object>>) GsonUtility
							.readJsonFileToObject(dependenciesFile, new TypeToken<List<Map<String, Object>>>() {
							}.getType());
					// List<String> dependentEngineIds = (List<String>)
					// GsonUtility.readJsonFileToObject(dependenciesFile, new
					// TypeToken<List<String>>() {}.getType());
					if (projectDependencies != null && !projectDependencies.isEmpty()) {
						List<String> dependentEngineIds = new ArrayList<>();
						for (Map<String, Object> dep : projectDependencies) {
							dependentEngineIds.add((String) dep.get("engine_id"));
						}
						SecurityProjectUtils.updateProjectDependenciesWithoutType(user, projectId, dependentEngineIds);
					}
					// delete this file since values can update and file is dynamically generated on
					// export
					dependenciesFile.delete();
				}
			}

			logger.info(step + ") Done");
		} catch (Exception e) {
			error = true;
			classLogger.error("Error occurred trying to synchronize the metadata and insights for the zip file", e);
			throw new SemossPixelException(
					"Error occurred trying to synchronize the metadata and insights for the zip file", false);
		} finally {
			if (error) {
				// delete all the resources
				cleanUpFolders(randomTempUnzipF, finalProjectSmssF, finalProjectFolderF);
				// remove from DIHelper
				if (projectAddedToDIHelper) {
					UploadUtilities.removeProjectFromDIHelper(projectId);
				}
				// delete from security
				SecurityProjectUtils.deleteProject(projectId);
			}
		}

		// add user as engine owner
		List<AuthProvider> logins = user.getLogins();
		for (AuthProvider ap : logins) {
			SecurityProjectUtils.addProjectOwner(user, projectId, user.getAccessToken(ap).getId());
		}

		ClusterUtil.pushProject(projectId);

		Map<String, Object> retMap = UploadUtilities.getProjectReturnData(this.insight.getUser(), projectId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	/**
	 * 
	 * @param fileToDelete
	 */
	private void cleanUpFolders(File... fileToDelete) {
		for (File f : fileToDelete) {
			if (f != null && f.exists()) {
				try {
					FileUtils.forceDelete(f);
				} catch (IOException e) {
					classLogger.error("Error on clean up attempting to delete " + f.getAbsolutePath(), e);
				}
			}
		}
	}

}
