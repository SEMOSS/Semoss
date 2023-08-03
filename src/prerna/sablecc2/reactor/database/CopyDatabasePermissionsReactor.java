package prerna.sablecc2.reactor.database;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;

public class CopyDatabasePermissionsReactor extends AbstractReactor {

	// TODO: make equivalent for project permissions
	// TODO: make equivalent for project permissions
	// TODO: make equivalent for project permissions
	// TODO: make equivalent for project permissions
	// TODO: make equivalent for project permissions

	private static final Logger logger = LogManager.getLogger(CopyDatabasePermissionsReactor.class);
	
	private static String SOURCE_DATABASE = "sourceDatabase";
	private static String TARGET_DATABASE = "targetDatabase";
	
	public CopyDatabasePermissionsReactor() {
		this.keysToGet = new String[]{ SOURCE_DATABASE, TARGET_DATABASE };
	}

	@Override
	public NounMetadata execute() {
		// must have security enabled to do this
		if(!AbstractSecurityUtils.securityEnabled()) {
			throw new IllegalArgumentException("Security must be activiated to perform this operation");
		}

		organizeKeys();
		String sourceDatabaseId = this.keyValue.get(this.keysToGet[0]);
		String targetDatabaseId = this.keyValue.get(this.keysToGet[1]);

		// must be an editor for both to run this
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), sourceDatabaseId)) {
			throw new IllegalArgumentException("You do not have edit access to the source database");
		}
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), targetDatabaseId)) {
			throw new IllegalArgumentException("You do not have edit access to the target database");
		}
		
		// now perform the operation
		try {
			SecurityEngineUtils.copyDatabasePermissions(sourceDatabaseId, targetDatabaseId);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred copying the app permissions.  Detailed error: " + e.getMessage());
		}

		String sourceDatabase = SecurityEngineUtils.getDatabaseAliasForId(sourceDatabaseId);
		String targetDatabase = SecurityEngineUtils.getDatabaseAliasForId(targetDatabaseId);

		return new NounMetadata("Copied permissions from database " 
				+ sourceDatabase  + "__" + sourceDatabaseId + " to " + targetDatabase + "__" + targetDatabaseId, 
				PixelDataType.CONST_STRING);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(SOURCE_DATABASE)) {
			return "The database id that is used to provide information";
		} else if(key.equals(TARGET_DATABASE)) {
			return "The database id that the operation is applied on";
		}
		return ReactorKeysEnum.getDescriptionFromKey(key);
	}
}
