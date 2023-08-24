package prerna.sablecc2.reactor.database;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;

public class CopyEnginePermissionsReactor extends AbstractReactor {

	// TODO: make equivalent for project permissions
	// TODO: make equivalent for project permissions
	// TODO: make equivalent for project permissions
	// TODO: make equivalent for project permissions
	// TODO: make equivalent for project permissions

	private static final Logger logger = LogManager.getLogger(CopyEnginePermissionsReactor.class);
	
	private static final String SOURCE_ENGINE = "sourceEngine";
	private static final String TARGET_ENGINE = "targetEngine";
	
	public CopyEnginePermissionsReactor() {
		this.keysToGet = new String[]{ SOURCE_ENGINE, TARGET_ENGINE };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String sourceEngineId = this.keyValue.get(this.keysToGet[0]);
		String targetEngineId = this.keyValue.get(this.keysToGet[1]);

		// must be an editor for both to run this
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), sourceEngineId)) {
			throw new IllegalArgumentException("You do not have edit access to the source engine");
		}
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), targetEngineId)) {
			throw new IllegalArgumentException("You do not have edit access to the target engine");
		}
		
		// now perform the operation
		try {
			SecurityEngineUtils.copyEnginePermissions(sourceEngineId, targetEngineId);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred copying the engine permissions.  Detailed error: " + e.getMessage());
		}

		String sourceDatabase = SecurityEngineUtils.getEngineAliasForId(sourceEngineId);
		String targetDatabase = SecurityEngineUtils.getEngineAliasForId(targetEngineId);

		return new NounMetadata("Copied permissions from database " 
				+ sourceDatabase  + "__" + sourceEngineId + " to " + targetDatabase + "__" + targetEngineId, 
				PixelDataType.CONST_STRING);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(SOURCE_ENGINE)) {
			return "The engine id that is used to provide information";
		} else if(key.equals(TARGET_ENGINE)) {
			return "The engine id that the operation is applied on";
		}
		return ReactorKeysEnum.getDescriptionFromKey(key);
	}
}
