package prerna.util.git.reactors;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
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
		
		// you can only push
		// if you are the owner
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database does not exist or user does not have access to edit database");
		}
		String databaseName = SecurityEngineUtils.getEngineAliasForId(databaseId);
		
		List<Map<String, String>> fileInfo = GitUtils.getStatus(databaseId, databaseName);
		return new NounMetadata(fileInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.MARKET_PLACE);
	}

}
