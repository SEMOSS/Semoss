package prerna.auth.utils.reactors.admin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminGetEngineUsageReactor extends AbstractReactor {

	
	public AdminGetEngineUsageReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
		Map<String, Object> retMap = new HashMap<>();
		List<Map<String, Object>> overAllInfoForEngineList = ModelInferenceLogsUtils.getOverAllEngineUsageFromModelInferenceLogs(engineId);
		List<Map<String, Object>> tokenUsagePerProjectList = ModelInferenceLogsUtils.getTokenUsagePerProjectForEngine(engineId);
		List<Map<String, Object>> tokenUsagePerUserList = ModelInferenceLogsUtils.getUserUsagePerEngine(engineId);
		retMap.put("OVERALL_USUAGE", overAllInfoForEngineList);
		retMap.put("TOKEN_USAGE_PER_PROJECT", tokenUsagePerProjectList);
		retMap.put("TOKEN_USAGE_PER_USER", tokenUsagePerUserList);
		return new NounMetadata(retMap, PixelDataType.FORMATTED_DATA_SET);
	}

}
