package prerna.sablecc2.reactor.app;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;

public class CopyAppPermissionsReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(CopyAppPermissionsReactor.class);
	
	private static String SOURCE_APP = "sourceApp";
	private static String TARGET_APP = "targetApp";

	public CopyAppPermissionsReactor() {
		this.keysToGet = new String[]{ SOURCE_APP, TARGET_APP };
	}

	@Override
	public NounMetadata execute() {
		// must have security enabled to do this
		if(!AbstractSecurityUtils.securityEnabled()) {
			throw new IllegalArgumentException("Security must be activiated to perform this operation");
		}

		organizeKeys();
		String sourceAppId = this.keyValue.get(this.keysToGet[0]);
		String targetAppId = this.keyValue.get(this.keysToGet[1]);

		// must be an editor for both to run this
		if(!SecurityAppUtils.userCanEditDatabase(this.insight.getUser(), sourceAppId)) {
			throw new IllegalArgumentException("You do not have edit access to the source database");
		}
		if(!SecurityAppUtils.userCanEditDatabase(this.insight.getUser(), targetAppId)) {
			throw new IllegalArgumentException("You do not have edit access to the target database");
		}
		
		// now perform the operation
		try {
			SecurityAppUtils.copyDatabasePermissions(sourceAppId, targetAppId);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured copying the app permissions.  Detailed error: " + e.getMessage());
		}

		String sourceApp = SecurityQueryUtils.getDatabaseAliasForId(sourceAppId);
		String targetApp = SecurityQueryUtils.getDatabaseAliasForId(targetAppId);

		return new NounMetadata("Copied permissions from app " + sourceApp  + "__" + sourceAppId + " to " + targetApp + "__" + targetAppId, PixelDataType.CONST_STRING);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(SOURCE_APP)) {
			return "The app id that is used to provide information";
		} else if(key.equals(TARGET_APP)) {
			return "The app id that the operation is applied on";
		}
		return ReactorKeysEnum.getDescriptionFromKey(key);
	}
}
