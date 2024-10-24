package prerna.engine.impl.model;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class LLMReactor extends AbstractReactor {
	
	public LLMReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.CONTEXT.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}

		Map<String, Object> validationData = tokenResponseTimeValidation(engineId, user);

		String question = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));
		String context = this.keyValue.get(this.keysToGet[2]);
		if (context != null) {
			context = Utility.decodeURIComponent(context);
		}

		Map<String, Object> paramMap = getMap();
		IModelEngine modelEngine = Utility.getModel(engineId);
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}

		// reverting this for now
//		if (paramMap.containsKey("full_prompt")) {
//			paramMap.put("full_prompt", Utility.decodeURIComponent((String) paramMap.get("full_prompt")));
//		}

		Map<String, Object> output = modelEngine.ask(question, context, this.insight, paramMap).toMap();
		validationData.put(Constants.TOTAL_TOKENS_RESPONSE_TIME,
				Utility.nullCheckUtility(output.get(Constants.NUMBER_OF_TOKENS_IN_RESPONSE), Integer.class)
						+  Utility.nullCheckUtility(output.get(Constants.NUMBER_OF_TOKENS_IN_PROMPT), Integer.class) 
						+  Utility.nullCheckUtility(validationData.get(Constants.TOTAL_TOKENS_RESPONSE_TIME), Integer.class));
		// Add the validation results to the output
		output.put(Constants.RATE_LIMIT, validationData);
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	
	/**
	 *@param engineId
	 *@param user
	 * @return validation data
	 */
	private Map<String, Object> tokenResponseTimeValidation(String engineId, User user) {
		// Obtain EnginePermission based on User and engineId
		List<Map<String, Object>> enginePermission = SecurityEngineUtils.getEngineUsersPermissionObject(user, engineId);

	    int maxTokens = 0;
	    double maxResponseTime = 0.0;
	    String frequency = null;
	    boolean isMaxToken = true;
	    
		// Check if permissions exist
		if (enginePermission != null && !enginePermission.isEmpty()) {
			// If EnginePermission is available, use its values
			Map<String, Object> permission = enginePermission.get(0);
			isMaxToken = Utility.nullCheckUtility(permission.get(Constants.ENGINE_IS_MAX_TOKEN), Boolean.class);
			// Check if engine's max token or response time has a valid value
			if (Utility.nullCheckUtility(permission.get(Constants.ENGINE_MAX_TOKEN), Integer.class) > 0
					|| Utility.nullCheckUtility(permission.get(Constants.ENGINE_MAX_RESPONSE_TIME), Double.class) > 0.0) {
				maxTokens =   Utility.nullCheckUtility(permission.get(Constants.ENGINE_MAX_TOKEN), Integer.class);
				maxResponseTime =  Utility.nullCheckUtility(permission.get(Constants.ENGINE_MAX_RESPONSE_TIME), Double.class) ;
				frequency = Utility.nullCheckUtility(permission.get(Constants.ENGINE_FREQUENCY), String.class);
			} else {
				// Fall back to user-specific values
				maxTokens = Utility.nullCheckUtility(permission.get(Constants.USER_MAX_TOKEN),Integer.class);
				maxResponseTime =  Utility.nullCheckUtility( permission.get(Constants.USER_MAX_RESPONSE_TIME), Double.class);
				frequency = Utility.nullCheckUtility(permission.get(Constants.USER_FREQUENCY),String.class);
			}
		}
		//get the current date and time in  UTC format
		LocalDateTime localDateTime = LocalDateTime.now();
		ZonedDateTime currentDateTime = localDateTime.atZone(ZoneId.of(Constants.UTC_ZONE));
        
		//retrieve total tokens or total response time
		Map<String, Object> totalTokenResponseMap = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(user, currentDateTime,
				frequency, isMaxToken);
		Map<String, Object> validationData = new HashMap<>();
		if (isMaxToken) {
			int totalTokens = Utility.nullCheckUtility(totalTokenResponseMap.get(Constants.TOTAL_TOKEN_RESPONSE), Integer.class);
			// Check if max limits are reached
			boolean isMaxTokenReached = totalTokens > maxTokens;
			// Exception handling for token limits
			if (isMaxTokenReached) {
				throw new IllegalArgumentException(String.format(Constants.TOKEN_LIMIT_EXCEEDED_MESSAGE, totalTokens, maxTokens));
			}
			validationData.put(Constants.IS_MAX_TOKEN_REACHED, isMaxTokenReached);
			validationData.put(Constants.TOTAL_TOKENS_RESPONSE_TIME, totalTokens);
			validationData.put(Constants.MAX_TOKEN, maxTokens);
		} else {
			// Check if total response time exceeds the max allowed
			double totalResponseTime = Utility.nullCheckUtility(totalTokenResponseMap.get(Constants.TOTAL_RESPONSE_TIME), Double.class);
			boolean isMaxResponseTimeReached = totalResponseTime > maxResponseTime;

			// Exception handling for response time limits
			if (isMaxResponseTimeReached) {
				throw new IllegalArgumentException(String.format(Constants.RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE, totalResponseTime, maxResponseTime));
			}
			validationData.put(Constants.IS_MAX_RESPONSE_TIME_REACHED, isMaxResponseTimeReached);
			validationData.put(Constants.TOTAL_TOKENS_RESPONSE_TIME, totalResponseTime);
			validationData.put(Constants.MAX_RESPONSE_TIME, maxResponseTime);
		}
		validationData.put(Constants.IS_MAX_TOKEN, isMaxToken);
		return validationData;
	}
		

	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(keysToGet[3]);
        if(mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if(mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if(mapInputs != null && !mapInputs.isEmpty()) {
            return (Map<String, Object>) mapInputs.get(0).getValue();
        }
        return null;
    }
	
}
