package prerna.util.git.reactors;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.git.GitRepoUtils;

public class ListAppRemotes extends AbstractReactor {

	/**
	 * Get the list of remotes for a given database
	 */
	
	public ListAppRemotes() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if(databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Need to provide the database id");
		}
		
		String databaseName = null;
		
		// you can only push
		// if you are the owner
		if(AbstractSecurityUtils.securityEnabled()) {
			databaseId = SecurityQueryUtils.testUserDatabaseIdForAlias(this.insight.getUser(), databaseId);
			if(!SecurityAppUtils.userCanEditDatabase(this.insight.getUser(), databaseId)) {
				throw new IllegalArgumentException("Database does not exist or user does not have access to edit database");
			}
			databaseName = SecurityQueryUtils.getDatabaseAliasForId(databaseId);
		} else {
			databaseName = MasterDatabaseUtility.getDatabaseAliasForId(databaseId);
		}
		
		String dbAssetFolder = AssetUtility.getProjectAssetVersionFolder(databaseName, databaseId);;	

		Logger logger = getLogger(this.getClass().getName());
		logger.info("Getting remotes configures on " + dbAssetFolder);
		
		List<Map<String, String>> repoList = GitRepoUtils.listConfigRemotes(dbAssetFolder);
		return new NounMetadata(repoList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.MARKET_PLACE);
	}
}
