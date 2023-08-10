package prerna.sablecc2.reactor.database.upload.rdf;

import java.io.File;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.rdf.RdfUploadReactorUtility;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

public class RdfReplaceDatabaseLoaderSheetUploadReactor extends RdfLoaderSheetUploadReactor {

	public RdfReplaceDatabaseLoaderSheetUploadReactor() {
		this.keysToGet = new String[] { 
				UploadInputUtility.DATABASE, 
				UploadInputUtility.FILE_PATH,
				UploadInputUtility.SPACE,
				UploadInputUtility.CUSTOM_BASE_URI
		};
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

		this.logger = getLogger(this.getClass().getName());

		organizeKeys();
		String databaseId = UploadInputUtility.getDatabaseNameOrId(this.store);
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
				NounMetadata noun = new NounMetadata("User must be signed into an account in order to create or update a database", PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
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
			databaseId = SecurityQueryUtils.testUserEngineIdForAlias(user, databaseId);

			// throw error is user is not owner
			if (!SecurityEngineUtils.userIsOwner(user, databaseId)) {
				NounMetadata noun = new NounMetadata("User must be the owner in order to replace all the data in the database", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		} else {
			// check if input is alias since we are adding ot existing
			databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(databaseId);
			if (!MasterDatabaseUtility.getAllDatabaseIds().contains(databaseId)) {
				throw new IllegalArgumentException("Database " + databaseId + " does not exist");
			}
		}

		try {
			this.databaseId = databaseId;
			this.databaseName = MasterDatabaseUtility.getDatabaseAliasForId(this.databaseId);
			// get existing database
			this.logger.info("Get existing database");
			this.database = Utility.getDatabase(databaseId, true);
			if (this.database == null) {
				throw new IllegalArgumentException("Couldn't find the database " + databaseId + " to append data into");
			}
			this.logger.info("Done");
			RdfUploadReactorUtility.deleteAllTriples(this.database);
			addToExistingDatabase(filePath);
			// NO NEED TO SYNC THE METADATA SINCE WE ARE ASSUMING IT IS THE SAME OWL IN THE REPLACE!
			//			this.logger.info("Process database metadata to allow for traversing across databases");
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
		ClusterUtil.reactorPushDatabase(this.databaseId);

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), this.databaseId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

}
