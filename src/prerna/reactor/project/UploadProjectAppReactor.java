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
import prerna.util.Utility;
import prerna.util.ZipUtils;
import prerna.util.gson.GsonUtility;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

public class UploadProjectAppReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(UploadProjectAppReactor.class);

	private static final String CLASS_NAME = UploadProjectAppReactor.class.getName();

	public UploadProjectAppReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.GLOBAL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = this.getLogger(CLASS_NAME);
		int step = 1;
		String zipFilePath = UploadInputUtility.getFilePath(this.store, this.insight);
		// do we want this project to be accessible to everyone
		boolean global = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.GLOBAL.getKey())+"");
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

		// creating a temp folder to unzip project folder and smss
		String randomIdAsDir = UUID.randomUUID().toString();
		String projectFolderPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + Constants.PROJECT_FOLDER;
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
				if (filePath.endsWith(Constants.SEMOSS_EXTENSION)) {
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
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error occurred while unzipping the files", false);
		} finally {
			if (error) {
				cleanUpFolders(randomTempUnzipF);
			}
		}

		String projects = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
		String projectId = null;
		String projectName = null;
		File finalProjectSmssF = null;
		File finalProjectFolderF = null;
		try {
			logger.info(step + ") Reading smss");
			Properties prop = Utility.loadProperties(smssFileLoc);
			projectId = prop.getProperty(Constants.PROJECT);
			projectName = Utility.normalizePath(prop.getProperty(Constants.PROJECT_ALIAS));
			
			logger.info(step + ") Done");
			step++;

			finalProjectFolderF = new File(Utility.normalizePath(projectFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(projectName, projectId)));
			finalProjectSmssF = new File(Utility.normalizePath(projectFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(projectName, projectId) + Constants.SEMOSS_EXTENSION));

			// need to ignore file watcher
			if (!(projects.startsWith(projectId) || projects.contains(";" + projectId + ";")
					|| projects.endsWith(";" + projectId))) {
				String newProjects = projects + ";" + projectId;
				DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, newProjects);
			} else {
				SemossPixelException exception = new SemossPixelException(
						NounMetadata.getErrorNounMessage("Project id already exists"));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
			
			// create the project folder
			// since this assumes we have only the assets
			// make the project name / app root / version / asset folder path
			File finalProjectVersionF = new File(finalProjectFolderF.getAbsolutePath() 
					+ DIR_SEPARATOR + Constants.APP_ROOT_FOLDER 
					+ DIR_SEPARATOR + Constants.VERSION_FOLDER);
			File finalProjectAssetF = new File(finalProjectVersionF.getAbsolutePath()
					+ DIR_SEPARATOR + Constants.ASSETS_FOLDER);
			finalProjectAssetF.mkdirs();

			// move project folder
			logger.info(step + ") Moving project folder");
			File[] allFiles = randomTempUnzipF.listFiles();
			for(File f : allFiles) {
				if(f.getName().equals("assets") && f.isDirectory()) {
					// we move the assets folder into the version folder
					FileUtils.copyDirectory(f, finalProjectAssetF);
				} else if(f.isDirectory()) {
					FileUtils.copyDirectory(f, finalProjectVersionF);
				} else if(f.isFile()) {
					// this way the metadata files are in the same location
					// if it is UploadProject or UploadProjectApp
					FileUtils.copyFileToDirectory(f, finalProjectFolderF, true);
				}
			}
			logger.info(step + ") Done");

			step++;
			// move smss file which would have been already copied into the project folder 
			logger.info(step + ") Moving smss file");
			File tempUnzippedSmssF = new File(Utility.normalizePath(
					finalProjectFolderF + DIR_SEPARATOR + SmssUtilities.getUniqueName(projectName, projectId) + Constants.SEMOSS_EXTENSION));
			FileUtils.moveFile(tempUnzippedSmssF, finalProjectSmssF);
			logger.info(step + ") Done");
			step++;

		} catch (Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage(), false);
		} finally {
			if (error) {
				// remove from DIHelper
				UploadUtilities.removeProjectFromDIHelper(projectId);
				cleanUpFolders(randomTempUnzipF, finalProjectSmssF, finalProjectFolderF);
			} else {
				// just delete the temp project folder
				cleanUpFolders(randomTempUnzipF);
			}
		}

		try {
			DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, finalProjectSmssF.getAbsolutePath());
			logger.info(step + ") Grabbing project insights");
			SecurityProjectUtils.addProject(projectId, global, user);
			
			// see if we have any dependencies or metadata to load
			{
				File metadataFile = new File(finalProjectFolderF.getAbsolutePath() + "/" + projectName + IEngine.METADATA_FILE_SUFFIX);
				if(metadataFile.exists() && metadataFile.isFile()) {
					Map<String, Object> metadata = (Map<String, Object>) GsonUtility.readJsonFileToObject(metadataFile, new TypeToken<Map<String, Object>>() {}.getType());
					SecurityProjectUtils.updateProjectMetadata(projectId, metadata);
					// delete this file since values can update and file is dynamically generated on export
					metadataFile.delete();
				}
				
				File dependenciesFile = new File(finalProjectFolderF.getAbsolutePath() + "/" + projectName + IProject.DEPENDENCIES_FILE_SUFFIX);
				if(dependenciesFile.exists() && dependenciesFile.isFile()) {
					List<Map<String, Object>> projectDependencies = (List<Map<String, Object>>) GsonUtility.readJsonFileToObject(dependenciesFile, new TypeToken<List<Map<String, Object>>>() {}.getType());
					// List<String> dependentEngineIds = (List<String>) GsonUtility.readJsonFileToObject(dependenciesFile, new TypeToken<List<String>>() {}.getType());
					if(projectDependencies != null && !projectDependencies.isEmpty()) {
						List<String> dependentEngineIds = new ArrayList<>();
						for(Map<String, Object> dep : projectDependencies) {
							dependentEngineIds.add((String) dep.get("engine_id"));
						}
						SecurityProjectUtils.updateProjectDependencies(user, projectId, dependentEngineIds);
					}
					// delete this file since values can update and file is dynamically generated on export
					dependenciesFile.delete();
				}
			}
			
			logger.info(step + ") Done");
		} catch (Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"Error occurred trying to synchronize the metadata and insights for the zip file", false);
		} finally {
			if (error) {
				// delete all the resources
				cleanUpFolders(randomTempUnzipF, finalProjectSmssF, finalProjectFolderF);
				// remove from DIHelper
				UploadUtilities.removeProjectFromDIHelper(projectId);
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
		for(File f : fileToDelete) {
			if(f != null && f.exists()) {
				try {
					FileUtils.forceDelete(f);
				} catch (IOException e) {
					classLogger.warn("Error on clean up attempting to delete " + f.getAbsolutePath());
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

}
