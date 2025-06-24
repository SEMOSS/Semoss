package prerna.auth.utils.reactors.admin;

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
import prerna.util.Utility;

public class AdminGetEngineUsagePerProjectReactor extends AbstractReactor {

	public AdminGetEngineUsagePerProjectReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.START_DATE.getKey(), ReactorKeysEnum.END_DATE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		if (!Utility.isModelInferenceLogsEnabled()) {
			throw new IllegalArgumentException("Model inference logs database must be functioning for this report to be generated");
		}

		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
		String limit = this.keyValue.get(this.keysToGet[1]);
		String offset = this.keyValue.get(this.keysToGet[2]);
		String startDate = this.keyValue.get(ReactorKeysEnum.START_DATE.getKey());
		String endDate = this.keyValue.get(ReactorKeysEnum.END_DATE.getKey());

		List<Map<String, Object>> tokenUsagePerProjectList = ModelInferenceLogsUtils.getTokenUsagePerProjectForEngine(engineId, limit, offset, startDate, endDate);

		return new NounMetadata(tokenUsagePerProjectList, PixelDataType.FORMATTED_DATA_SET);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns the number of tokens usage per project for an engine. The fields for this report include: number_of_tokens, number_of_requests, project_name, project_id.";
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
