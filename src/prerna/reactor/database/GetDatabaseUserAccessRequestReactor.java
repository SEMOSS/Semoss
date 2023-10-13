package prerna.reactor.database;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

@Deprecated
public class GetDatabaseUserAccessRequestReactor extends AbstractReactor {
	
	public GetDatabaseUserAccessRequestReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if(databaseId == null) {
			throw new IllegalArgumentException("Please define the database id.");
		}
		// check user permission for the database
		User user = this.insight.getUser();
		if(!SecurityEngineUtils.userCanEditEngine(user, databaseId)) {
			throw new IllegalArgumentException("User does not have permission to view access requests for this database");
		}
		List<Map<String, Object>> requests = SecurityEngineUtils.getUserAccessRequestsByEngine(databaseId);
		return new NounMetadata(requests, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}
}
