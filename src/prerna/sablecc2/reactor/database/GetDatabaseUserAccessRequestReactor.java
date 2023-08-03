package prerna.sablecc2.reactor.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

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
		List<Map<String, Object>> requests = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			requests = SecurityEngineUtils.getUserAccessRequestsByDatabase(databaseId);
		} else {
			requests = new ArrayList<>();
		}
		return new NounMetadata(requests, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}
}
