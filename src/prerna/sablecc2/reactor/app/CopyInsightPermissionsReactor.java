package prerna.sablecc2.reactor.app;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUserDatabaseUtils;
import prerna.auth.utils.SecurityUserInsightUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;

public class CopyInsightPermissionsReactor extends AbstractReactor {

	// TODO: RENAME APP -> PROJECT
	
	private static final Logger logger = LogManager.getLogger(CopyInsightPermissionsReactor.class);
	
	private static String SOURCE_APP = "sourceApp";
	private static String SOURCE_INSIGHT = "sourceInsight";

	private static String TARGET_APP = "targetApp";
	private static String TARGET_INSIGHT = "targetInsight";

	public CopyInsightPermissionsReactor() {
		this.keysToGet = new String[]{ SOURCE_APP, SOURCE_INSIGHT, TARGET_APP, TARGET_INSIGHT };
	}

	@Override
	public NounMetadata execute() {
		// must have security enabled to do this
		if(!AbstractSecurityUtils.securityEnabled()) {
			throw new IllegalArgumentException("Security must be activiated to perform this operation");
		}

		organizeKeys();
		String sourceAppId = this.keyValue.get(this.keysToGet[0]);
		String sourceInsightId = this.keyValue.get(this.keysToGet[1]);
		String targetAppId = this.keyValue.get(this.keysToGet[2]);
		String targetInsightId = this.keyValue.get(this.keysToGet[3]);

		// must be an editor for both to run this
		if(!SecurityUserDatabaseUtils.userCanEditDatabase(this.insight.getUser(), sourceAppId)) {
			throw new IllegalArgumentException("You do not have edit access to the source database");
		}
		if(!SecurityUserDatabaseUtils.userCanEditDatabase(this.insight.getUser(), targetAppId)) {
			throw new IllegalArgumentException("You do not have edit access to the target database");
		}
		
		String sourceInsightName = SecurityQueryUtils.getInsightNameForId(sourceAppId, sourceInsightId);
		String targetInsightName = SecurityQueryUtils.getInsightNameForId(targetAppId, targetInsightId);
		
		// if the insight name is null, then it doesn't exist
		if(sourceInsightName == null) {
			throw new IllegalArgumentException("Could not find the insight defined in the source app");
		}
		if(targetInsightName == null) {
			throw new IllegalArgumentException("Could not find the insight defined in the target app");
		}
		
		// now perform the operation
		try {
			SecurityUserInsightUtils.copyInsightPermissions(sourceAppId, sourceInsightId, targetAppId, targetInsightId);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured copying the insight permissions.  Detailed error: " + e.getMessage());
		}

		String sourceApp = SecurityQueryUtils.getDatabaseAliasForId(sourceAppId);
		String targetApp = SecurityQueryUtils.getDatabaseAliasForId(targetAppId);
		
		return new NounMetadata("Copied permissions from app " + sourceApp  + "__" + sourceAppId 
				+ " insight \"" + sourceInsightName + "\" to " + targetApp + "__" + targetAppId 
				+ " insight \"" + targetInsightName + "\"", PixelDataType.CONST_STRING);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(SOURCE_APP)) {
			return "The app id that is used to provide information";
		} else if(key.equals(TARGET_APP)) {
			return "The app id that the operation is applied on";
		} else if(key.equals(SOURCE_INSIGHT)) {
			return "The insight id in the source app to provide information";
		} else if(key.equals(TARGET_INSIGHT)) {
			return "The insight id in the target app that the operation is applied on";
		}
		return ReactorKeysEnum.getDescriptionFromKey(key);
	}
}
