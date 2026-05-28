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
import prerna.engine.impl.SmssUtilities;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.UploadInputUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import prerna.util.gson.GsonUtility;

public class UploadProjectAppReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(UploadProjectAppReactor.class);

	private static final String CLASS_NAME = UploadProjectAppReactor.class.getName();

	public static final String CREATE_MODE = "create";
	public static final String REPLACE_MODE = "replace";

	public static final String MODE_KEY = "mode";

	public UploadProjectAppReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.GLOBAL.getKey(), MODE_KEY };
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
					NounMetadata.getErrorNounMessage("User can upload an app but cannot make the app public"));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// creating a temp folder to unzip project folder and smss
		String randomIdAsDir = UUID.randomUUID().toString();
		String projectFolderPath = EngineUtility.getLocalEngineBaseDirectory(IEngine.CATALOG_TYPE.PROJECT);
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

		boolean replace = deleteIfExisting();
		String projects = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
		String projectId = null;
		String projectName = null;
		File finalProjectSmssF = null;
		File finalProjectFolderF = null;
		File finalProjectVersionF = null;
		File finalProjectAssetF = null;
		boolean projectAddedToDIHelper = false;
		try {
			logger.info(step + ") Reading smss");
			Properties prop = Utility.loadProperties(smssFileLoc);
			projectId = prop.getProperty(Constants.PROJECT);
			projectName = Utility.normalizePath(prop.getProperty(Constants.PROJECT_ALIAS));

			logger.info(step + ") Done");
			step++;

			finalProjectFolderF = new File(Utility.normalizePath(
					projectFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(projectName, projectId)));
			finalProjectSmssF = new File(Utility.normalizePath(projectFolderPath + DIR_SEPARATOR
					+ SmssUtilities.getUniqueName(projectName, projectId) + Constants.SEMOSS_EXTENSION));

			// is this an update or first time upload
			if (SecurityProjectUtils.projectExists(projectId)) {
				// this is an update
				// do we allow update?
				// if yes, do you have access to update?
				if (replace) {
					if (!SecurityProjectUtils.userIsOwner(user, projectId)) {
						cleanUpFolders(randomTempUnzipF);
						SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage(
								"User is not an owner to replace the existing project with id = " + projectId));
						exception.setContinueThreadOfExecution(false);
						throw exception;
					} else {
						// make sure we pull the project from cloud
						IProject project = Utility.getProject(projectId);
						project.close();
						// now delete the project folder - we will make a new one when we copy from temp
						// dir
						FileUtils.deleteDirectory(finalProjectFolderF);
						// now delete the project smss - we will make a new one when we copy from temp
						// dir
						FileUtils.delete(finalProjectSmssF);
					}
				} else {
					cleanUpFolders(randomTempUnzipF);
					SemossPixelException exception = new SemossPixelException(
							NounMetadata.getErrorNounMessage("Project id already exists"));
					exception.setContinueThreadOfExecution(false);
					throw exception;
				}
			} else {
				// first time upload
				// need to ignore file watcher
				if (!(projects.startsWith(projectId) || projects.contains(";" + projectId + ";")
						|| projects.endsWith(";" + projectId))) {
					String newProjects = projects + ";" + projectId;
					DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, newProjects);
					projectAddedToDIHelper = true;
				}
			}

			// create the project folder
			// since this assumes we have only the assets
			// make the project name / app root / version / asset folder path
			finalProjectVersionF = new File(finalProjectFolderF.getAbsolutePath() + DIR_SEPARATOR
					+ Constants.APP_ROOT_FOLDER + DIR_SEPARATOR + Constants.VERSION_FOLDER);
			finalProjectAssetF = new File(
					finalProjectVersionF.getAbsolutePath() + DIR_SEPARATOR + Constants.ASSETS_FOLDER);
			finalProjectAssetF.mkdirs();

			// move project folder
			logger.info(step + ") Moving project folder");
			File[] allFiles = randomTempUnzipF.listFiles();
			for (File f : allFiles) {
				if (f.getName().equals("assets") && f.isDirectory()) {
					// we move the assets folder into the version folder
					FileUtils.copyDirectory(f, finalProjectAssetF);
				} else if (f.isDirectory()) {
					FileUtils.copyDirectory(f, finalProjectVersionF);
				} else if (f.isFile()) {
					// this way the metadata files are in the same location
					// if it is UploadProject or UploadProjectApp
					FileUtils.copyFileToDirectory(f, finalProjectFolderF, true);
				}
			}
			logger.info(step + ") Done");

			step++;
			// move smss file which would have been already copied into the project folder
			logger.info(step + ") Moving smss file");
			File tempUnzippedSmssF = new File(Utility.normalizePath(finalProjectFolderF + DIR_SEPARATOR
					+ SmssUtilities.getUniqueName(projectName, projectId) + Constants.SEMOSS_EXTENSION));
			FileUtils.moveFile(tempUnzippedSmssF, finalProjectSmssF);
			logger.info(step + ") Done");
			step++;

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
			logger.info(step + ") Grabbing project insights");
			if (!replace) {
				SecurityProjectUtils.addProject(projectId, global, user);
			}

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

			// see if we have any metadata to load
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
			}

			logger.info(step + ") Done");
		} catch (Exception e) {
			error = true;
			classLogger.error(Constants.STACKTRACE, e);
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
			} else {
				File[] assetsFilesToDelete = finalProjectAssetF
						.listFiles((dir, name) -> name.endsWith(IEngine.METADATA_FILE_SUFFIX)
								|| name.endsWith(IProject.DEPENDENCIES_FILE_SUFFIX)
								|| name.endsWith(Constants.SEMOSS_EXTENSION));
				cleanUpFolders(assetsFilesToDelete);
			}
		}

		// add user as engine owner
		if (!replace) {
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityProjectUtils.addProjectOwner(user, projectId, user.getAccessToken(ap).getId());
			}
		}

		// push new project to cloud
		ClusterUtil.pushProject(projectId);

		Map<String, Object> engineIdMap = ProjectHelper.extractEngineIdsFromProjectFolder(projectId,
				finalProjectFolderF);
		// update the project dependencies table only with valid engineIds
		if (engineIdMap.containsKey("success")) {
			Map<String, Object> successMap = (Map<String, Object>) engineIdMap.get("success");

			List<Map<String, Object>> depEngines = new ArrayList<>();

			for (Map.Entry<String, Object> entry : successMap.entrySet()) {

				String engineId = entry.getKey();

				Map<String, Object> engineDetails = (Map<String, Object>) entry.getValue();

				String engineType = (String) engineDetails.get("engineType");

				Map<String, Object> depEngine = new HashMap<>();
				depEngine.put("ENGINEID", engineId);
				depEngine.put("ENGINETYPE", engineType);

				depEngines.add(depEngine);
			}

			SecurityProjectUtils.updateEngineDependencies(user, projectId, IEngine.CATALOG_TYPE.PROJECT.name(),
					depEngines);
		}

		// sending the success and failed list of engineIds to FE
		Map<String, Object> retMap = UploadUtilities.getProjectReturnData(this.insight.getUser(), projectId);
		retMap.put("engineIds", engineIdMap);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	/**
	 * This method is intended to be overridden by other reactors in case we want to
	 * have different default values if no mode is passed in
	 * 
	 * @return
	 */
	protected boolean deleteIfExisting() {
		String modeKey = this.keyValue.getOrDefault(MODE_KEY, CREATE_MODE).trim();
		if (REPLACE_MODE.equalsIgnoreCase(modeKey)) {
			return true;
		}
		return false;
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

	@Override
	public String getReactorDescription() {
		return "Import an app from an exported zip smss-app file";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "This is a required value containing the relative file path of the single zip file to be imported";
		} else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
		} else if (key.equals(ReactorKeysEnum.GLOBAL.getKey())) {
			return "This is a required value to determine if the app is public or private";
		} else if (key.equals(MODE_KEY)) {
			return """
					Optional paramter that is either 'create' or 'replace'.
					'create' is the default and will break if the app id already exists.
					'replace' will replace if the app id exist but user must be an owner of the app.
					Default is 'create' if no value is passed in.
					""";
		}
		return super.getDescriptionForKey(key);
	}

}