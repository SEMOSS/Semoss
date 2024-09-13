package prerna.engine.impl.model.inferencetracking;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetEngineUsagePerUserReactor extends AbstractReactor {
	
	public GetEngineUsagePerUserReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.START_DATE.getKey(), ReactorKeysEnum.END_DATE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);
		if(!SecurityEngineUtils.userIsOwner(user, engineId)) {
			throw new IllegalArgumentException("Engine does not exist or user is not an owner of Engine");
		}
		
		
		String limit = this.keyValue.get(this.keysToGet[1]);
		String offset = this.keyValue.get(this.keysToGet[2]);
		String startDate = this.keyValue.get(ReactorKeysEnum.START_DATE.getKey());
		String endDate = this.keyValue.get(ReactorKeysEnum.END_DATE.getKey());
		
		List<Map<String, Object>> tokenUsagePerUserList = ModelInferenceLogsUtils.getUserUsagePerEngine(engineId, limit, offset, startDate, endDate);

		return new NounMetadata(tokenUsagePerUserList, PixelDataType.FORMATTED_DATA_SET);
	}

}
