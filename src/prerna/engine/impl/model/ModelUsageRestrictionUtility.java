package prerna.engine.impl.model;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.util.Constants;
import prerna.util.Utility;

public final class ModelUsageRestrictionUtility {

	private static final Logger classLogger = LogManager.getLogger(ModelUsageRestrictionUtility.class);
	
	public static Map<String, Object> getModelUsageRestriction(User user, String engineId) {
		Map<String, Object> userRestrictionMap = new HashMap<>();

		List<Map<String, Object>> engineUserPermission = SecurityEngineUtils.getEngineUsagePermissionMap(user, engineId);
		if(engineUserPermission != null && !engineUserPermission.isEmpty()) {
			// there should only 1 row in this object
			Map<String, Object> engineUserPermissionMap = engineUserPermission.get(0);
			// lets see if any restriction is applied
			
			String userLvlModelUsageRestriction = (String) engineUserPermissionMap.get(Constants.USER_USAGE_RESTRICTION_KEY);
			String userLvlModelUsageFrequency = (String) engineUserPermissionMap.get(Constants.USER_MODEL_USAGE_FREQUENCY_KEY);
			Number userLvlModelUsageMaxTokens = (Number) engineUserPermissionMap.get(Constants.USER_MODEL_MAX_TOKEN_KEY);
			Number userLvlModelUsageMaxResponseTime = (Number) engineUserPermissionMap.get(Constants.USER_MODEL_MAX_RESPONSE_TIME_KEY);
			
			String engineLvlModelUsageRestriction = (String) engineUserPermissionMap.get(Constants.ENGINE_USAGE_RESTRICTION_KEY);
			String engineLvlModelUsageFrequency = (String) engineUserPermissionMap.get(Constants.ENGINE_USAGE_FREQUENCY_KEY);
			Number engineLvlModelUsageMaxTokens = (Number) engineUserPermissionMap.get(Constants.ENGINE_MAX_TOKEN_KEY);
			Number engineLvlModelUsageMaxResponseTime = (Number) engineUserPermissionMap.get(Constants.ENGINE_MAX_RESPONSE_TIME_KEY);

			ZonedDateTime currentDateTime = Utility.getCurrentZonedDateTimeUTC();
			
			Number currentUsage = null;
			// engine specific restriction
			if(engineLvlModelUsageRestriction != null && !engineLvlModelUsageRestriction.isEmpty()) {
				if(Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(engineLvlModelUsageRestriction)) {
					currentUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, engineId, currentDateTime, engineLvlModelUsageFrequency);
					
					if(currentUsage.intValue() > engineLvlModelUsageMaxTokens.intValue()) {
						throw new IllegalArgumentException(String.format(Constants.ENGINE_TOKEN_LIMIT_EXCEEDED_MESSAGE, currentUsage.intValue(), engineLvlModelUsageMaxTokens.intValue()));
					}
					
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE, Constants.MODEL_TOKEN_RESTRICTION_VALUE);
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, currentUsage.intValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE, engineLvlModelUsageMaxTokens.intValue());

				} else if(Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equalsIgnoreCase(engineLvlModelUsageRestriction)) {
					currentUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE, user, engineId, currentDateTime, engineLvlModelUsageFrequency);
					
					if(currentUsage.doubleValue() > engineLvlModelUsageMaxResponseTime.doubleValue()) {
						throw new IllegalArgumentException(String.format(Constants.ENGINE_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE, currentUsage.doubleValue(), engineLvlModelUsageMaxResponseTime.doubleValue()));
					}
					
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE, Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE);
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, currentUsage.intValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE, engineLvlModelUsageMaxResponseTime.intValue());
					
				} else {
					classLogger.warn("Unknown engine level model restriction type = '"+engineLvlModelUsageRestriction+"' for user = " + User.getSingleLogginName(user));
				}
			}
			// user general restriction
			else if(userLvlModelUsageRestriction != null && !userLvlModelUsageRestriction.isEmpty()) {
				if(Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(userLvlModelUsageRestriction)) {

					currentUsage = ModelInferenceLogsUtils.getTotalUsageForUser(Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, engineId, currentDateTime, userLvlModelUsageFrequency);
					
					if(currentUsage.intValue() > userLvlModelUsageMaxTokens.intValue()) {
						throw new IllegalArgumentException(String.format(Constants.USER_TOKEN_LIMIT_EXCEEDED_MESSAGE, currentUsage.intValue(), userLvlModelUsageMaxTokens.intValue()));
					}
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE, Constants.MODEL_TOKEN_RESTRICTION_VALUE);
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, currentUsage.intValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE, userLvlModelUsageMaxTokens.intValue());

				} else if(Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equalsIgnoreCase(userLvlModelUsageRestriction)) {

					currentUsage = ModelInferenceLogsUtils.getTotalUsageForUser(Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE,user, engineId, currentDateTime, userLvlModelUsageFrequency);
					
					if(currentUsage.doubleValue() > userLvlModelUsageMaxResponseTime.doubleValue()) {
						throw new IllegalArgumentException(String.format(Constants.USER_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE, currentUsage.doubleValue(), userLvlModelUsageMaxResponseTime.doubleValue()));
					}
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE, Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE);
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, currentUsage.intValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE, userLvlModelUsageMaxResponseTime.intValue());
					
				} else {
					classLogger.warn("Unknown user level model restriction type = '"+userLvlModelUsageRestriction+"' for user = " + User.getSingleLogginName(user));
				}
			}
		}
		
		return userRestrictionMap;
	}
	
	/**
	 * 
	 * @param userRestrictionMap
	 * @param askModelResponse
	 * @param inputTime
	 * @param outputTime
	 */
	public static void updateRestrictionMapCurrentUsage(Map<String, Object> userRestrictionMap, AbstractModelEngineResponse<?> modelResponse, ZonedDateTime inputTime, ZonedDateTime outputTime) {
		if(userRestrictionMap != null && !userRestrictionMap.isEmpty()) {
			String restrictionMode = (String) userRestrictionMap.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE);

			if(Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(restrictionMode)) {
				userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, 
						// put in the new value of the current usage we calculated + the number of tokens we just created
						((Number) userRestrictionMap.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE)).intValue()
						+ modelResponse.getNumberOfTokensInPrompt() + modelResponse.getNumberOfTokensInResponse()
						);

			} else if(Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equals(restrictionMode)) {
				
				Duration duration = Duration.between(inputTime, outputTime);
		        long millisecondsDifference = duration.toMillis();
		        Double millisecondsDouble = (double) millisecondsDifference;

				
				userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, 
						// put in the new value of the current usage we calculated + the time for this new response
						((Number) userRestrictionMap.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE)).doubleValue()
						+ millisecondsDouble
						);
			}
			
			// now add this to the model response
			modelResponse.setUsageRestriction(userRestrictionMap);
		}
	}
	
	private ModelUsageRestrictionUtility() {
		
	}
}
