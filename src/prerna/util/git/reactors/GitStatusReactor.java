package prerna.util.git.reactors;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitUtils;

public class GitStatusReactor extends AbstractReactor {

	public GitStatusReactor() {
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
			databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
			if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
				throw new IllegalArgumentException("Database does not exist or user does not have access to edit database");
			}
			databaseName = SecurityEngineUtils.getDatabaseAliasForId(databaseId);
		} else {
			databaseName = MasterDatabaseUtility.getDatabaseAliasForId(databaseId);
		}
		
		List<Map<String, String>> fileInfo = GitUtils.getStatus(databaseId, databaseName);
		return new NounMetadata(fileInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.MARKET_PLACE);
	}

}
