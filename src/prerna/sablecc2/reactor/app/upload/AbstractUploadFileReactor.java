package prerna.sablecc2.reactor.app.upload;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class AbstractUploadFileReactor extends AbstractReactor {

	/**
	 * Every reactor that extends this needs to define its own inputs
	 * However, every one needs to have the following in the keysToGet array:
	 * UploadUtility.APP
	 * UploadInputUtility.FILE_PATH
	 * UploadInputUtility.ADD_TO_EXISTING
	 * 
	 */
	
	// we need to define some variables that are stored at the class level
	// so that we can properly account for cleanup if errors occur
	protected transient Logger logger;
	protected transient String appId;
	protected transient String appName;
	protected transient IEngine engine;
	protected transient File appFolder;
	protected transient File tempSmss;
	protected transient File smssFile;
	
	protected transient boolean error = false;
	
	@Override
	public NounMetadata execute() {
		this.logger = getLogger(this.getClass().getName());

		organizeKeys();
		String appIdOrName = UploadInputUtility.getAppName(this.store);
		String filePath = UploadInputUtility.getFilePath(this.store);
		if (!new File(filePath).exists()) {
			throw new IllegalArgumentException("Could not find the specified file to use for importing");
		}
		final boolean existing = UploadInputUtility.getExisting(this.store);
		// check security
		User user = null;
		boolean security = AbstractSecurityUtils.securityEnabled();
		if (security) {
			user = this.insight.getUser();
			if (user == null) {
				NounMetadata noun = new NounMetadata("User must be signed into an account in order to create or update an app", PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}

		if (existing) {
			if (security) {
				// check if input is alias since we are adding ot existing
				appIdOrName = SecurityQueryUtils.testUserEngineIdForAlias(user, appIdOrName);
				if (!SecurityQueryUtils.userCanEditEngine(user, appIdOrName)) {
					NounMetadata noun = new NounMetadata("User does not have sufficient priviledges to create or update an app", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
					SemossPixelException err = new SemossPixelException(noun);
					err.setContinueThreadOfExecution(false);
					throw err;
				}
			} else {
				// check if input is alias since we are adding ot existing
				appIdOrName = MasterDatabaseUtility.testEngineIdIfAlias(appIdOrName);
				if (!MasterDatabaseUtility.getAllEngineIds().contains(appIdOrName)) {
					throw new IllegalArgumentException("Database " + appIdOrName + " does not exist");
				}
			}

			try {
				this.appId = appIdOrName;
				this.appName = MasterDatabaseUtility.getEngineAliasForId(this.appId);
				addToExistingApp(appIdOrName, filePath);
			} catch (Exception e) {
				e.printStackTrace();
				this.error = true;
				if (e instanceof SemossPixelException) {
					throw (SemossPixelException) e;
				} else {
					NounMetadata noun = new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING,
							PixelOperationType.ERROR);
					SemossPixelException err = new SemossPixelException(noun);
					err.setContinueThreadOfExecution(false);
					throw err;
				}
			} finally {
				closeFileHelpers();
				// need to rollback
				// TODO:
			}
		} else {
			try {
				// make a new id
				this.appId = UUID.randomUUID().toString();
				this.appName = appIdOrName;
				// validate app
				this.logger.info("Start validating app");
				UploadUtilities.validateApp(user, this.appName, this.appId);
				this.logger.info("Done validating app");
				// create app folder
				this.logger.info("Start generating app folder");
				this.appFolder = UploadUtilities.generateAppFolder(this.appId, this.appName);
				this.logger.info("Complete");
				generateNewApp(user, this.appId, this.appName, filePath);
			} catch (Exception e) {
				e.printStackTrace();
				this.error = true;
				if (e instanceof SemossPixelException) {
					throw (SemossPixelException) e;
				} else {
					NounMetadata noun = new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR);
					SemossPixelException err = new SemossPixelException(noun);
					err.setContinueThreadOfExecution(false);
					throw err;
				}
			} finally {
				closeFileHelpers();
				if (this.error) {
					// need to delete everything...
					cleanUpCreateNewError();
				}
			}

			// even if no security, just add user as engine owner
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider ap : logins) {
					SecurityUpdateUtils.addEngineOwner(this.appId, user.getAccessToken(ap).getId());
				}
			}
		}

		// if we got here
		// no errors
		// we can do normal clean up of files
		// TODO:

		ClusterUtil.reactorPushApp(this.appId);

		Map<String, Object> retMap = UploadUtilities.getAppReturnData(this.insight.getUser(), this.appId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	/**
	 * Delete all the corresponding files that are generated from the upload the failed
	 */
	private void cleanUpCreateNewError() {
		// TODO:clean up DIHelper!
		try {
			// close the DB so we can delete it
			if (this.engine != null) {
				engine.closeDB();
			}

			// delete the .temp file
			if (this.tempSmss != null && this.tempSmss.exists()) {
				FileUtils.forceDelete(this.tempSmss);
			}
			// delete the .smss file
			if (this.smssFile != null && this.smssFile.exists()) {
				FileUtils.forceDelete(this.smssFile);
			}
			// delete the engine folder and all its contents
			if (this.appFolder != null && this.appFolder.exists()) {
				File[] files = this.appFolder.listFiles();
				if (files != null) { // some JVMs return null for empty dirs
					for (File f : files) {
						FileUtils.forceDelete(f);
					}
				}
				FileUtils.forceDelete(this.appFolder);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	///////////////////////////////////////////////////////

	/*
	 * Execution methods
	 * This will be done by every implementation of the upload file reactors
	 */

	public abstract void generateNewApp(User user, final String newAppId, final String newAppName, final String filePath) throws Exception;

	public abstract void addToExistingApp(final String appId, final String filePath) throws Exception;

	public abstract void closeFileHelpers();
}
