package prerna.util.git.reactors;

import org.apache.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.git.GitUtils;

public class IsGit extends AbstractReactor {

	public IsGit() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(this.getClass().getName());
		organizeKeys();
		String appId = keyValue.get(keysToGet[0]);
		if(appId == null || appId.isEmpty()) {
			throw new IllegalArgumentException("Need to specify the app name");
		}
		String appName = null;
		
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityAppUtils.userCanEditEngine(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("App does not exist or user does not have access to edit database");
			}
			appName = SecurityQueryUtils.getEngineAliasForId(appId);
		} else {
			appName = MasterDatabaseUtility.getEngineAliasForId(appId);
		}
		
		logger.info("Checking - Please wait");
		// get the path of the git location
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String appFolder = baseFolder + "/db/" + SmssUtilities.getUniqueName(appName, appId);
		boolean isGit = GitUtils.isGit(appFolder);
		logger.info("Complete");
		return new NounMetadata(isGit, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
	}

}
