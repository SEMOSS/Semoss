package prerna.solr.reactor;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseUsersReactor extends AbstractReactor {
	
	public DatabaseUsersReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		
		if(databaseId == null || (databaseId = databaseId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input an database id");
		}
		
		// make sure valid id for user
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
			// you dont have access
			throw new IllegalArgumentException("Database does not exist or user does not have access to database");
		}
		
		List<Map<String, Object>> baseInfo = SecurityEngineUtils.getDisplayDatabaseOwnersAndEditors(databaseId);
		return new NounMetadata(baseInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_USERS);
	}

}