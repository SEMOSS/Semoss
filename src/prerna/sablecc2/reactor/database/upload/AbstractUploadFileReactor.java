package prerna.sablecc2.reactor.database.upload;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

public abstract class AbstractUploadFileReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AbstractUploadFileReactor.class);
	
	/**
	 * Every reactor that extends this needs to define its own inputs
	 * However, every one needs to have the following in the keysToGet array:
	 * UploadUtility.DATABASE
	 * UploadInputUtility.FILE_PATH
	 * UploadInputUtility.ADD_TO_EXISTING
	 * 
	 */
	
	// we need to define some variables that are stored at the class level
	// so that we can properly account for cleanup if errors occur
	protected transient Logger logger;
	protected transient String databaseId;
	protected transient String databaseName;
	protected transient IDatabaseEngine database;
	protected transient IProject project;
	protected transient File databaseFolder;
	protected transient File tempSmss;
	protected transient File smssFile;
	
	protected transient boolean error = false;
	
	@Override
	public NounMetadata execute() {
		this.logger = getLogger(this.getClass().getName());

		organizeKeys();
		String databaseIdOrName = UploadInputUtility.getDatabaseNameOrId(this.store);
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		if (!new File(filePath).exists()) {
			throw new IllegalArgumentException("Could not find the specified file to use for importing");
		}
		final boolean existing = UploadInputUtility.getExisting(this.store);
		// check security
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to create or update a database", PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}
		
		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
			throwAnonymousUserError();
		}
		
		// throw error is user doesn't have rights to publish new databases
		if (AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(this.insight.getUser())) {
			throwUserNotPublisherError();
		}
		
		if (AbstractSecurityUtils.adminOnlyEngineAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		if (existing) {
			// check if input is alias since we are adding ot existing
			databaseIdOrName = SecurityQueryUtils.testUserEngineIdForAlias(user, databaseIdOrName);
			if (!SecurityEngineUtils.userCanEditEngine(user, databaseIdOrName)) {
				NounMetadata noun = new NounMetadata("User does not have sufficient priviledges to create or update a database", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}

			try {
				this.databaseId = databaseIdOrName;
				this.databaseName = MasterDatabaseUtility.getDatabaseAliasForId(this.databaseId);
				// get existing database
				this.logger.info("Get existing database");
				this.database = Utility.getDatabase(databaseId);
				if (this.database == null) {
					throw new IllegalArgumentException("Couldn't find the database " + databaseId + " to append data into");
				}
				this.logger.info("Done");
				addToExistingDatabase(filePath);
				// sync metadata
				this.logger.info("Process database metadata to allow for traversing across databases");
				UploadUtilities.updateMetadata(this.database.getEngineId(), user);
				this.logger.info("Complete");
				this.logger.info("Delete OWL position map");
				File owlF = this.database.getOwlPositionFile();
				if(owlF.exists()) {
					owlF.delete();
				}
				this.logger.info("Complete");
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
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
				this.databaseId = UUID.randomUUID().toString();
				this.databaseName = databaseIdOrName;
				// validate database
				this.logger.info("Start validating database");
				UploadUtilities.validateDatabase(user, this.databaseName, this.databaseId);
				this.logger.info("Done validating database");
				// create database folder
				this.logger.info("Start generating database folder");
				this.databaseFolder = UploadUtilities.generateDatabaseFolder(this.databaseId, this.databaseName);
				this.logger.info("Complete");
				generateNewDatabase(user, this.databaseName, filePath);
				// and rename .temp to .smss
				this.smssFile = new File(this.tempSmss.getAbsolutePath().replace(".temp", ".smss"));
				FileUtils.copyFile(this.tempSmss, this.smssFile);
				this.tempSmss.delete();
				this.database.setSmssFilePath(this.smssFile.getAbsolutePath());
				UploadUtilities.updateDIHelper(this.databaseId, this.databaseName, this.database, this.smssFile);
				// sync metadata
				this.logger.info("Process database metadata to allow for traversing across databases");
				UploadUtilities.updateMetadata(this.databaseId, user);
				
				// adding all the git here
				// make a version folder if one doesn't exist
				/*
					String versionFolder = 	AssetUtility.getAppAssetVersionFolder(databaseName, databaseId);
					File file = new File(versionFolder);
					if(!file.exists())
						file.mkdir();
					// I will assume the directory is there now
					GitRepoUtils.init(versionFolder);
				*/
				
				this.logger.info("Complete");
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
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

			// even if no security, just add user as database owner
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider ap : logins) {
					SecurityEngineUtils.addDatabaseOwner(this.databaseId, user.getAccessToken(ap).getId());
				}
			}
		}

		// if we got here
		// no errors
		// we can do normal clean up of files
		// TODO:

		ClusterUtil.pushDatabase(this.databaseId);

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), this.databaseId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	/**
	 * Delete all the corresponding files that are generated from the upload the failed
	 */
	private void cleanUpCreateNewError() {
		try {
			// close the DB so we can delete it
			if (this.database != null) {
				database.close();
			}

			// delete the .temp file
			if (this.tempSmss != null && this.tempSmss.exists()) {
				FileUtils.forceDelete(this.tempSmss);
			}
			// delete the .smss file
			if (this.smssFile != null && this.smssFile.exists()) {
				FileUtils.forceDelete(this.smssFile);
			}
			// delete the database folder and all its contents
			if (this.databaseFolder != null && this.databaseFolder.exists()) {
				File[] files = this.databaseFolder.listFiles();
				if (files != null) { // some JVMs return null for empty dirs
					for (File f : files) {
						FileUtils.forceDelete(f);
					}
				}
				FileUtils.forceDelete(this.databaseFolder);
			}
			
			UploadUtilities.removeEngineFromDIHelper(this.databaseId);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	///////////////////////////////////////////////////////

	/*
	 * Execution methods
	 * This will be done by every implementation of the upload file reactors
	 */

	public abstract void generateNewDatabase(User user, final String newDatabaseName, final String filePath) throws Exception;

	public abstract void addToExistingDatabase(final String filePath) throws Exception;

	public abstract void closeFileHelpers();
}
