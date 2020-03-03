package prerna.sablecc2.reactor.app.upload.gremlin.file;

import java.io.File;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.engine.impl.tinker.TinkerUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Utility;

public class TinkerReplaceDatabaseCsvUploadReactor extends TinkerCsvUploadReactor {
	
	public TinkerReplaceDatabaseCsvUploadReactor() {
		this.keysToGet = new String[] { 
				UploadInputUtility.APP, 
				UploadInputUtility.FILE_PATH,
				UploadInputUtility.SPACE,
				UploadInputUtility.ADD_TO_EXISTING,
				UploadInputUtility.DELIMITER, 
				UploadInputUtility.DATA_TYPE_MAP, 
				UploadInputUtility.NEW_HEADERS,
				UploadInputUtility.METAMODEL, 
				UploadInputUtility.PROP_FILE, 
				UploadInputUtility.ADD_TO_EXISTING,
				UploadInputUtility.START_ROW, 
				UploadInputUtility.END_ROW, 
				UploadInputUtility.ADDITIONAL_DATA_TYPES,
				TINKER_DRIVER_TYPE };
	}
	
	@Override
	public NounMetadata execute() {
		/*
		 * THIS LOGIC IS THE SAME AS THE LOGIC IN THE AbstractUploadFileReactor
		 * EXCEPT IT CAN ONLY BE FOR OVERRIDING AN EXISTING APP
		 * THE LOGIC IS THE SAME EXCEPT THERE IS AN ADDITIONAL METHOD
		 * TO REMOVE THE APP DATABASE BEFORE RUNNING THE UPDATE
		 * 
		 */

		this.logger = getLogger(this.getClass().getName());

		organizeKeys();
		String appId = UploadInputUtility.getAppNameOrId(this.store);
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		if (!new File(filePath).exists()) {
			throw new IllegalArgumentException("Could not find the specified file to use for importing");
		}
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

			// throw error if user is anonymous
			if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}

		if (security) {
			// check if input is alias since we are adding ot existing
			appId = SecurityQueryUtils.testUserEngineIdForAlias(user, appId);

			// throw error is user is not owner
			if (!SecurityAppUtils.userIsOwner(user, appId)) {
				NounMetadata noun = new NounMetadata("User must be the owner in order to replace all the data in the app", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		} else {
			// check if input is alias since we are adding ot existing
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
			if (!MasterDatabaseUtility.getAllEngineIds().contains(appId)) {
				throw new IllegalArgumentException("Database " + appId + " does not exist");
			}
		}

		try {
			this.appId = appId;
			this.appName = MasterDatabaseUtility.getEngineAliasForId(this.appId);
			// get existing app
			this.logger.info("Get existing app");
			this.engine = Utility.getEngine(appId, true, true);
			if (this.engine == null) {
				throw new IllegalArgumentException("Couldn't find the app " + appId + " to append data into");
			}
			if (!(this.engine instanceof TinkerEngine)) {
				throw new IllegalArgumentException("App must be using a tinker database");
			}
			this.logger.info("Done");
			TinkerUtilities.removeAllVertices((TinkerEngine) engine);
			addToExistingApp(filePath);
			// NO NEED TO SYNC THE METADATA SINCE WE ARE ASSUMING IT IS THE SAME OWL IN THE REPLACE!
			//			this.logger.info("Process app metadata to allow for traversing across apps");
			//			UploadUtilities.updateMetadata(this.engine.getEngineId());
			this.logger.info("Complete");
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

		// if we got here
		// no errors
		// we can do normal clean up of files
		// TODO:
		ClusterUtil.reactorPushApp(this.appId);

		Map<String, Object> retMap = UploadUtilities.getAppReturnData(this.insight.getUser(), this.appId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
}
