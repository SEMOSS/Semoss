package prerna.util.git.reactors;

import java.util.List;
import java.util.Map;

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
import prerna.util.git.GitRepoUtils;

public class ListAppRemotes extends AbstractReactor {

	/**
	 * Get the list of remotes for a given app
	 */
	
	public ListAppRemotes() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		if(appId == null || appId.isEmpty()) {
			throw new IllegalArgumentException("Need to provide the app name");
		}
		
		String appName = null;
		
		// you can only push
		// if you are the owner
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityAppUtils.userCanEditEngine(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("App does not exist or user does not have access to edit database");
			}
			appName = SecurityQueryUtils.getEngineAliasForId(appId);
		} else {
			appName = MasterDatabaseUtility.getEngineAliasForId(appId);
		}
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String dbName = baseFolder + "/db/" + SmssUtilities.getUniqueName(appName, appId) + "/version";	

		Logger logger = getLogger(this.getClass().getName());
		logger.info("Getting remotes configures on " + dbName);
		
		List<Map<String, String>> repoList = GitRepoUtils.listConfigRemotes(dbName);
		return new NounMetadata(repoList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.MARKET_PLACE);
	}
}
