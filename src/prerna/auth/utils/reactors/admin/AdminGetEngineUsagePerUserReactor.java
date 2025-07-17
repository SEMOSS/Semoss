package prerna.auth.utils.reactors.admin;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AdminGetEngineUsagePerUserReactor extends AbstractReactor {

	public AdminGetEngineUsagePerUserReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.START_DATE.getKey(), ReactorKeysEnum.END_DATE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		
		if (!Utility.isModelInferenceLogsEnabled()) {
			throw new IllegalArgumentException("Model inference logs database must be enabled to create this report");
		}

		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Must define the engine id for the usage details");
		}

		String limit = this.keyValue.get(this.keysToGet[1]);
		String offset = this.keyValue.get(this.keysToGet[2]);	
		String startDate = this.keyValue.get(ReactorKeysEnum.START_DATE.getKey());
		String endDate = this.keyValue.get(ReactorKeysEnum.END_DATE.getKey());
		
		List<Map<String, Object>> tokenUsagePerUserList = ModelInferenceLogsUtils.getUserUsagePerEngine(engineId, limit, offset, startDate, endDate);

		return new NounMetadata(tokenUsagePerUserList, PixelDataType.FORMATTED_DATA_SET);
	}
	
	@Override
	public String getReactorDescription() {
		return """
				This reactor returns the number of tokens usage for an engine. 
				The fields for this report include: user_name, user_id, number_of_messages, number_of_rooms, number_of_tokens.
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) { 
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine id for the report";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Limit to the number of results to be returned";
		} else if (key.equals(ReactorKeysEnum.OFFSET.getKey())) {
			return "Offset to the number of results to be returned";
		} else if (key.equals(ReactorKeysEnum.START_DATE.getKey())) {
			return "Start date filter on the query executed";
		} else if (key.equals(ReactorKeysEnum.END_DATE.getKey())) {
			return "End date filter on the query executed";
		}
		return super.getDescriptionForKey(key);
	}
	
}
