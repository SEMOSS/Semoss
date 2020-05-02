package prerna.sablecc2.reactor.app.upload.r;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.io.Files;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.r.RNativeEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Utility;

public class RReplaceDatabaseCsvUploadReactor extends AbstractReactor {
	
	public RReplaceDatabaseCsvUploadReactor() {
		this.keysToGet = new String[] { UploadInputUtility.APP, UploadInputUtility.FILE_PATH, UploadInputUtility.SPACE };
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

		Logger logger = getLogger(this.getClass().getName());

		organizeKeys();
		String appId = UploadInputUtility.getAppNameOrId(this.store);
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		File newFile = new File(filePath);
		if (!newFile.exists()) {
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

		boolean error = false;
		File rEngineOldFile = null;
		File rEngineOldFileCopy = null;
		try {
			// get existing app
			logger.info("Get existing app");
			IEngine engine = Utility.getEngine(appId, true, true);
			if (engine == null) {
				throw new IllegalArgumentException("Couldn't find the app " + appId + " to append data into");
			}
			if(engine.getEngineType() != IEngine.ENGINE_TYPE.R) {
				throw new IllegalArgumentException("Engine must be an existing R engine");
			}
			logger.info("Done");
			
			// only need to delete the existing file and replace it with the new file
			// first make a copy of the existing file
			rEngineOldFile = SmssUtilities.getDataFile(engine.getProp());
			rEngineOldFileCopy = new File(rEngineOldFile.getAbsolutePath() + "_COPY");
			Files.copy(rEngineOldFile, rEngineOldFileCopy);
			
			// now delete the old file
			rEngineOldFile.delete();
			// move over the new one
			Files.copy(newFile, rEngineOldFile);
			
			// reload the r engine
			RNativeEngine rEngine = (RNativeEngine) engine;
			rEngine.reloadFile();
			
			// NO NEED TO SYNC THE METADATA SINCE WE ARE ASSUMING IT IS THE SAME OWL IN THE REPLACE!
			//			this.logger.info("Process app metadata to allow for traversing across apps");
			//			UploadUtilities.updateMetadata(this.engine.getEngineId());
			logger.info("Complete");
		} catch (Exception e) {
			logger.error("StackTrace: ", e);
			error = true;
			if (e instanceof SemossPixelException) {
				throw (SemossPixelException) e;
			} else {
				NounMetadata noun = new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		} finally {
			if(error) {
				// need to revert
				// delete the existing file
				if(rEngineOldFile != null && rEngineOldFile.exists()) {
					rEngineOldFile.delete();
				}
				// replace it
				try {
					Files.copy(rEngineOldFileCopy, rEngineOldFile);
				} catch (IOException e) {
					logger.error("StackTrace: ", e);
				}
			} else {
				// delete the copy
				if (rEngineOldFileCopy != null) {
					rEngineOldFileCopy.delete();
				}
			}
		}

		// if we got here
		// no errors
		// we can do normal clean up of files
		// TODO:
		ClusterUtil.reactorPushApp(appId);

		Map<String, Object> retMap = UploadUtilities.getAppReturnData(this.insight.getUser(), appId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

}
