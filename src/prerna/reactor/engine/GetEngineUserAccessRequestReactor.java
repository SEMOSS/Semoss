package prerna.reactor.engine;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetEngineUserAccessRequestReactor extends AbstractReactor {
	
	public GetEngineUserAccessRequestReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(engineId == null) {
			throw new IllegalArgumentException("Please define the engine id.");
		}
		// check user permission for the database
		User user = this.insight.getUser();
		if(!SecurityAdminUtils.userIsAdmin(user) && 
				!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException("User does not have permission to view access requests for this engine");
		}
		List<Map<String, Object>> requests = SecurityEngineUtils.getUserAccessRequestsByEngine(engineId);
		return new NounMetadata(requests, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.ENGINE_INFO);
	}
}
