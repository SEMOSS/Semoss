package prerna.sablecc2.reactor.app;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.SmssUtilities;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class UploadAppReactor extends AbstractInsightReactor {
	private static final String CLASS_NAME = UploadAppReactor.class.getName();

	public UploadAppReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = this.getLogger(CLASS_NAME);
		int step = 1;
		String zipFilePath = UploadInputUtility.getFilePath(this.store, this.insight);
		// check security
		User user = null;
		boolean security = AbstractSecurityUtils.securityEnabled();
		if (security) {
			user = this.insight.getUser();
			if (user == null) {
				NounMetadata noun = new NounMetadata(
						"User must be signed into an account in order to create or update an app",
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
		}

		// creating a temp folder to unzip db folder and smss
		String temporaryAppId = UUID.randomUUID().toString();
		String dbFolderPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db";
		String tempDbFolderPath = dbFolderPath + DIR_SEPARATOR + temporaryAppId;
		File tempDbFolder = new File(tempDbFolderPath);

		// gotta keep track of the smssFile and files unzipped
		Map<String, List<String>> filesAdded = new HashMap<>();
		List<String> fileList = new Vector<>();
		String smssFileLoc = null;
		File smssFile = null;
		// unzip files to temp db folder
		try {
			logger.info(step + ") Unzipping app");
			filesAdded = ZipUtils.unzip(zipFilePath, tempDbFolderPath);
			logger.info(step + ") Done");
			step++;
			
			// look for smss file
			fileList = filesAdded.get("FILE");
			logger.info(step + ") Searching for smss");
			for (String filePath : fileList) {
				if (filePath.endsWith(Constants.SEMOSS_EXTENSION)) {
					smssFileLoc = tempDbFolderPath + DIR_SEPARATOR + filePath;
					smssFile = new File(smssFileLoc);
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
				try {
					FileUtils.deleteDirectory(new File(tempDbFolderPath));
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
				SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage("Unable to find " + Constants.SEMOSS_EXTENSION + " file."));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}

		} catch (IOException e) {
			e.printStackTrace();
			try {
				FileUtils.deleteDirectory(new File(tempDbFolderPath));
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage("Unable to unzip files."));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		
		logger.info(step + ") Reading smss");
		Properties prop = Utility.loadProperties(smssFileLoc);
		String appId = prop.getProperty(Constants.ENGINE);
		String appName = prop.getProperty(Constants.ENGINE_ALIAS);
		logger.info(step + ") Done");
		step++;


		// zip file has the smss and db folder on the same level
		// need to move these files around
		String oldDbFolderPath = tempDbFolder + DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId);
		File oldDbFolder = new File(oldDbFolderPath);
		File newDbFolder = new File(dbFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId));
		File finalSmss = new File(dbFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId) + Constants.SEMOSS_EXTENSION);
		String engines = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		try {
			// need to ignore file watcher
			if (!(engines.startsWith(appId) || engines.contains(";" + appId + ";") || engines.endsWith(";" + appId))) {
				engines = engines + ";" + appId;
			} else {
				SemossPixelException exception = new SemossPixelException(
						NounMetadata.getErrorNounMessage("App ID already exists"));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
			// move database folder
			logger.info(step + ") Moving app folder");
			FileUtils.copyDirectory(oldDbFolder, newDbFolder);
			logger.info(step + ") Done");
			step++;

			// move smss file
			logger.info(step + ") Moving smss file");
			smssFile = new File(tempDbFolder + DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId)
			+ Constants.SEMOSS_EXTENSION);
			FileUtils.copyFile(smssFile, finalSmss);
			logger.info(step + ") Done");
			step++;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engines);
			try {
				FileUtils.deleteDirectory(tempDbFolder);
			} catch (IOException e) {

			}
		}

		DIHelper.getInstance().getCoreProp().setProperty(appId + "_" + Constants.STORE, finalSmss.getAbsolutePath());
		logger.info(step + ") Grabbing app structure");
		Utility.synchronizeEngineMetadata(appId);
		logger.info(step + ") Done");
		SecurityUpdateUtils.addApp(appId, !AbstractSecurityUtils.securityEnabled());
		// even if no security, just add user as engine owner
		if (user != null) {
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityUpdateUtils.addEngineOwner(appId, user.getAccessToken(ap).getId());
			}
		}

		ClusterUtil.reactorPushApp(appId);

		Map<String, Object> retMap = UploadUtilities.getAppReturnData(this.insight.getUser(), appId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);	}
}