package prerna.reactor.database.upload.r;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.google.common.io.Files;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.r.RNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

public class RReplaceDatabaseCsvUploadReactor extends AbstractReactor {
	
	public RReplaceDatabaseCsvUploadReactor() {
		this.keysToGet = new String[] { UploadInputUtility.DATABASE, UploadInputUtility.FILE_PATH, UploadInputUtility.SPACE };
	}
	
	@Override
	public NounMetadata execute() {
		/*
		 * THIS LOGIC IS THE SAME AS THE LOGIC IN THE AbstractUploadFileReactor
		 * EXCEPT IT CAN ONLY BE FOR OVERRIDING AN EXISTING DATABASE
		 * THE LOGIC IS THE SAME EXCEPT THERE IS AN ADDITIONAL METHOD
		 * TO REMOVE THE DATABASE BEFORE RUNNING THE UPDATE
		 * 
		 */

		Logger logger = getLogger(this.getClass().getName());

		organizeKeys();
		String databaseId = UploadInputUtility.getDatabaseNameOrId(this.store);
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		File newFile = new File(filePath);
		if (!newFile.exists()) {
			throw new IllegalArgumentException("Could not find the specified file to use for importing");
		}
		// check security
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to create or update a database", PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		// throw error if user is anonymous
		if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
			throwAnonymousUserError();
		}
		// check if input is alias since we are adding ot existing
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(user, databaseId);

		// throw error is user is not owner
		if (!SecurityEngineUtils.userIsOwner(user, databaseId)) {
			NounMetadata noun = new NounMetadata("User must be the owner in order to replace all the data in the database", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		boolean error = false;
		File rDatabaseOldFile = null;
		File rDatabaseOldFileCopy = null;
		try {
			// get existing database
			logger.info("Get existing database");
			IDatabaseEngine database = Utility.getDatabase(databaseId, true);
			if (database == null) {
				throw new IllegalArgumentException("Couldn't find the database " + databaseId + " to append data into");
			}
			if(database.getDatabaseType() != IDatabaseEngine.DATABASE_TYPE.R) {
				throw new IllegalArgumentException("Database must be an existing R database");
			}
			logger.info("Done");
			
			// only need to delete the existing file and replace it with the new file
			// first make a copy of the existing file
			rDatabaseOldFile = SmssUtilities.getDataFile(database.getSmssProp());
			rDatabaseOldFileCopy = new File(rDatabaseOldFile.getAbsolutePath() + "_COPY");
			Files.copy(rDatabaseOldFile, rDatabaseOldFileCopy);
			
			// now delete the old file
			rDatabaseOldFile.delete();
			// move over the new one
			Files.copy(newFile, rDatabaseOldFile);
			
			// reload the r database
			RNativeEngine rDatabase = (RNativeEngine) database;
			rDatabase.reloadFile();
			
			// NO NEED TO SYNC THE METADATA SINCE WE ARE ASSUMING IT IS THE SAME OWL IN THE REPLACE!
			//			this.logger.info("Process database metadata to allow for traversing across databases");
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
				if(rDatabaseOldFile != null && rDatabaseOldFile.exists()) {
					rDatabaseOldFile.delete();
				}
				// replace it
				try {
					Files.copy(rDatabaseOldFileCopy, rDatabaseOldFile);
				} catch (IOException e) {
					logger.error("StackTrace: ", e);
				}
			} else {
				// delete the copy
				if (rDatabaseOldFileCopy != null) {
					rDatabaseOldFileCopy.delete();
				}
			}
		}

		// if we got here
		// no errors
		// we can do normal clean up of files
		// TODO:
		ClusterUtil.pushEngine(databaseId);

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), databaseId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

}
