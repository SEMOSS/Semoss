package prerna.sablecc2.reactor.project;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.LegacyToProjectRestructurerHelper;
import prerna.engine.impl.SmssUtilities;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

public class UploadProjectReactor extends AbstractInsightReactor {

	private static final String CLASS_NAME = UploadProjectReactor.class.getName();

	public UploadProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = this.getLogger(CLASS_NAME);
		int step = 1;
		String zipFilePath = UploadInputUtility.getFilePath(this.store,this.insight);

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
		String temporaryProjectId = UUID.randomUUID().toString();
		String projectFolderPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR
				+ Constants.PROJECT_FOLDER;
		String tempProjectFolderPath = projectFolderPath + DIR_SEPARATOR + temporaryProjectId;
		File tempProjectFolder = new File(tempProjectFolderPath);

		// gotta keep track of the smssFile and files unzipped
		Map<String, List<String>> filesAdded = new HashMap<>();
		List<String> fileList = new Vector<>();
		String smssFileLoc = null;
		File smssFile = null;
		// unzip files to temp project folder
		boolean error = false;
		try {
			logger.info(step + ") Unzipping project");
			filesAdded = ZipUtils.unzip(zipFilePath, tempProjectFolderPath);
			logger.info(step + ") Done");
			step++;

			// look for smss file
			fileList = filesAdded.get("FILE");
			logger.info(step + ") Searching for smss");
			for (String filePath : fileList) {
				if (filePath.endsWith(Constants.SEMOSS_EXTENSION)) {
					smssFileLoc = tempProjectFolderPath + DIR_SEPARATOR + filePath;
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
				cleanUpFolders(null, null, null, null, tempProjectFolder, logger);
			}
		}

		String projects = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
		String projectId = null;
		String projectName = null;
		String projectType = null;
		boolean hasPortal = false;
		String portalName = null;
		String projectGitProvider = null;
		String projectGitCloneUrl = null;
		
		
		File tempSmss = null;
		File tempEngFolder = null;
		File finalSmss = null;
		File finalEngFolder = null;
		File appRootFolder = null;
		File versionFolder = null;
		Boolean isLegacy = false;
		try {
			logger.info(step + ") Reading smss");
			Properties prop = Utility.loadProperties(smssFileLoc);
			if (prop.getProperty(Constants.ENGINE) != null || prop.getProperty(Constants.ENGINE_ALIAS) != null
					|| prop.getProperty(Constants.ENGINE_TYPE) != null) {
				isLegacy = true;
			}
			
			// pull some properties out for creating an smss if legacy format
			if(isLegacy) {
				projectId = prop.getProperty(Constants.ENGINE);
				projectName = prop.getProperty(Constants.ENGINE_ALIAS);
				projectType = prop.getProperty(Constants.ENGINE_TYPE);
			} else {
				projectId = prop.getProperty(Constants.PROJECT);
				projectName = prop.getProperty(Constants.PROJECT_ALIAS);
				projectType = prop.getProperty(Constants.PROJECT_TYPE);
			}
			hasPortal = Boolean.parseBoolean(prop.getProperty(Settings.PUBLIC_HOME_ENABLE));
			portalName = prop.getProperty(Settings.PORTAL_NAME);
			projectGitProvider = prop.getProperty(Constants.PROJECT_GIT_PROVIDER);
			projectGitCloneUrl = prop.getProperty(Constants.PROJECT_GIT_CLONE);

			logger.info(step + ") Done");
			step++;

			// zip file has the smss and project folder on the same level
			// need to move these files around
			String oldProjectFolderPath = tempProjectFolderPath + DIR_SEPARATOR
					+ SmssUtilities.getUniqueName(projectName, projectId);
			tempEngFolder = new File(Utility.normalizePath(oldProjectFolderPath));
			finalEngFolder = new File(Utility.normalizePath(
					projectFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(projectName, projectId)));
			finalSmss = new File(Utility.normalizePath(projectFolderPath + DIR_SEPARATOR
					+ SmssUtilities.getUniqueName(projectName, projectId) + Constants.SEMOSS_EXTENSION));

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

			if (isLegacy) {
				legacyToProjectRestructurerHelper.userScanAndCopyInsightsDatabaseIntoNewProjectFolder(
						Utility.normalizePath(projectFolderPath + DIR_SEPARATOR
								+ SmssUtilities.getUniqueName(projectName, projectId)),
						Utility.normalizePath(oldProjectFolderPath), false);

				legacyToProjectRestructurerHelper.userScanAndCopyVersionsIntoNewProjectFolder(
						Utility.normalizePath(projectFolderPath + DIR_SEPARATOR
								+ SmssUtilities.getUniqueName(projectName, projectId)),
						Utility.normalizePath(oldProjectFolderPath), false);

				// move project folder
				logger.info(step + ") Done");
				step++;

				// move smss file
				tempSmss = SmssUtilities.createTemporaryProjectSmss(projectId, projectName, hasPortal, portalName, projectGitProvider, projectGitCloneUrl, null);
				FileUtils.copyFile(tempSmss, finalSmss);
				tempSmss.delete();
				logger.info(step + ") Done");
				step++;
			} else {
				// move project folder
				logger.info(step + ") Moving project folder");
				FileUtils.copyDirectory(tempEngFolder, finalEngFolder);
				logger.info(step + ") Done");
				step++;
				// move smss file
				logger.info(step + ") Moving smss file");
				tempSmss = new File(Utility.normalizePath(tempProjectFolder + DIR_SEPARATOR
						+ SmssUtilities.getUniqueName(projectName, projectId) + Constants.SEMOSS_EXTENSION));
				FileUtils.copyFile(tempSmss, finalSmss);
				logger.info(step + ") Done");
				step++;
			}

		} catch (Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage(), false);
		} finally {
			if (error) {
				DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projects);
				cleanUpFolders(tempSmss, finalSmss, tempEngFolder, finalEngFolder, tempProjectFolder, logger);
			} else {
				// just delete the temp project folder
				cleanUpFolders(null, null, null, null, tempProjectFolder, logger);
			}
		}

		try {
			DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, finalSmss.getAbsolutePath());
			logger.info(step + ") Grabbing project insights");
			SecurityProjectUtils.addProject(projectId, false, user);
			logger.info(step + ") Done");
		} catch (Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"Error occurred trying to synchronize the metadata and insights for the zip file", false);
		} finally {
			if (error) {
				// delete all the resources
				cleanUpFolders(tempSmss, finalSmss, tempEngFolder, finalEngFolder, tempProjectFolder, logger);
				// remove from DIHelper
				DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projects);
				// delete from security
				SecurityProjectUtils.deleteProject(projectId);
			}
		}

		// even if no security, just add user as engine owner
		if (user != null) {
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityProjectUtils.addProjectOwner(projectId, user.getAccessToken(ap).getId());
			}
		}

		ClusterUtil.pushProject(projectId);

		Map<String, Object> retMap = UploadUtilities.getProjectReturnData(this.insight.getUser(), projectId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	/**
	 * Utility method to delete resources that have to be cleaned up
	 * 
	 * @param tempSmss
	 * @param finalSmss
	 * @param tempEngDir
	 * @param finalEngDir
	 * @param tempDbDir
	 * @param logger
	 */
	private void cleanUpFolders(File tempSmss, File finalSmss, File tempEngDir, File finalEngDir, File tempDbDir,
			Logger logger) {
		if (tempSmss != null && tempSmss.exists()) {
			try {
				FileUtils.forceDelete(tempSmss);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		if (finalSmss != null && finalSmss.exists()) {
			try {
				FileUtils.forceDelete(finalSmss);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		if (tempEngDir != null && tempEngDir.exists()) {
			try {
				FileUtils.deleteDirectory(tempEngDir);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		if (finalEngDir != null && finalEngDir.exists()) {
			try {
				FileUtils.deleteDirectory(finalEngDir);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		if (tempDbDir != null && tempDbDir.exists()) {
			try {
				FileUtils.deleteDirectory(tempDbDir);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
	}

	private boolean fileExists(String filePath) {
		if (filePath == null || (filePath = filePath.trim()).isEmpty()) {
			throw new IllegalArgumentException("Filepath is empty.");
		}
		File file = new File(filePath);
		if (file.isFile()) {
			return true;
		}
		return false;
	}
}
