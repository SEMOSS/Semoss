package prerna.reactor.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class EngineUsageProjectReactor extends AbstractReactor {
	public EngineUsageProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.AGENT_ID.getKey(), ReactorKeysEnum.START_DATE.getKey(),
				ReactorKeysEnum.END_DATE.getKey(), ReactorKeysEnum.TYPE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String agentId = this.keyValue.get(ReactorKeysEnum.AGENT_ID.getKey());
		String startDate = this.keyValue.get(ReactorKeysEnum.START_DATE.getKey());
		String endDate = this.keyValue.get(ReactorKeysEnum.END_DATE.getKey());
		String type = this.keyValue.get(ReactorKeysEnum.TYPE.getKey());

		List<Map<String, Object>> report = new ArrayList<>();
		Map<String, Object> resultMap = new HashMap<>();

		if ("App".equalsIgnoreCase(type)) {
			report = ModelInferenceLogsUtils.getModelInferenceAppReport(agentId, startDate, endDate);
			resultMap.put("appReport", report);
		} else if ("User".equalsIgnoreCase(type)) {
			report = ModelInferenceLogsUtils.getModelInferenceUserReport(agentId, startDate, endDate);
			resultMap.put("userReport", report);
		} else {
			resultMap.put("error", "Invalid type. Use 'App' or 'User'");
		}

		NounMetadata retNoun = new NounMetadata(resultMap, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.OPERATION);
		return retNoun;

	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.AGENT_ID.getKey())) {
			return "The ID of the agent (model) whose usage report is requested.";
		} else if (key.equals(ReactorKeysEnum.START_DATE.getKey())) {
			return "The start date for filtering the usage data (format: yyyy-MM-dd HH:mm:ss).";
		} else if (key.equals(ReactorKeysEnum.END_DATE.getKey())) {
			return "The end date for filtering the usage data (format: yyyy-MM-dd HH:mm:ss).";
		} else if (key.equals(ReactorKeysEnum.TYPE.getKey())) {
			return "The type of report to generate: either 'App' or 'User'.";
		}
		return super.getDescriptionForKey(key);
	}

}
