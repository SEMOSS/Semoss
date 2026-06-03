/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.impl.model;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEntityDefaultTokenUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityRoomTokenUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.util.Constants;
import prerna.util.Utility;

public final class ModelUsageRestrictionUtility {

	private static final Logger classLogger = LogManager.getLogger(ModelUsageRestrictionUtility.class);

	// exception Message for throttle limit
	public static final String USER_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Token limit exceeded for user level : You have used %d tokens, but the limit is %d";
	public static final String USER_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE = "Response time limit exceeded for user level : You have reached %.2f seconds, but the limit is %.2f seconds.";
	public static final String ENGINE_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Token limit exceeded for engine level: You have used %d tokens, but the limit is %d";
	public static final String ENGINE_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE = "Response time limit exceeded for engine level : You have reached %.2f seconds, but the limit is %.2f seconds.";
	public static final String ENGINE_INPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Input token limit exceeded for engine level: You have used %d input tokens, but the limit is %d";
	public static final String ENGINE_OUTPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Output token limit exceeded for engine level: You have used %d output tokens, but the limit is %d";
	public static final String PROJECT_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Token limit exceeded for project level: You have used %d tokens, but the limit is %d";
	public static final String PROJECT_INPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Input token limit exceeded for project level: You have used %d input tokens, but the limit is %d";
	public static final String PROJECT_OUTPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Output token limit exceeded for project level: You have used %d output tokens, but the limit is %d";
	public static final String PROJECT_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE = "Response time limit exceeded for project level : You have reached %.2f seconds, but the limit is %.2f seconds.";

	// Room-level token limit messages
	public static final String ROOM_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Token limit exceeded for room level: This room has used %d tokens, but the limit is %d";
	public static final String ROOM_INPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Input token limit exceeded for room level: This room has used %d input tokens, but the limit is %d";
	public static final String ROOM_OUTPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Output token limit exceeded for room level: This room has used %d output tokens, but the limit is %d";

	/**
	 * 
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static Map<String, Object> getModelUsageRestriction(User user, String engineId) {
		return getModelUsageRestriction(user, engineId, null, null);
	}

	/**
	 * 
	 * @param user
	 * @param engineId
	 * @param projectId
	 * @return
	 */
	public static Map<String, Object> getModelUsageRestriction(User user, String engineId, String projectId) {
		return getModelUsageRestriction(user, engineId, projectId, null);
	}

	/**
	 * Check all applicable usage restrictions for a user on a model engine in priority order of room, engine, project, user
	 *
	 *
	 * @param user      the requesting user
	 * @param engineId  the model engine id
	 * @param projectId optional project id for project-level checks
	 * @param roomId    optional room id for room-level token limit checks
	 * @return map with restriction mode and current/max values for the response payload
	 */
	public static Map<String, Object> getModelUsageRestriction(User user, String engineId, String projectId, String roomId) {
		Map<String, Object> userRestrictionMap = new HashMap<>();

		// room-level token limit check
		if (roomId != null && !roomId.trim().isEmpty()) {
			checkRoomLevelRestriction(user, roomId, userRestrictionMap);
		}

		List<Map<String, Object>> engineUserPermission = SecurityEngineUtils.getEngineUsagePermissionMap(user,
				engineId);
		if (engineUserPermission != null && !engineUserPermission.isEmpty()) {
			Map<String, Object> engineUserPermissionMap = engineUserPermission.get(0);

			String userLvlModelUsageRestriction = (String) engineUserPermissionMap
					.get(Constants.USER_USAGE_RESTRICTION_KEY);
			String userLvlModelUsageFrequency = (String) engineUserPermissionMap
					.get(Constants.USER_MODEL_USAGE_FREQUENCY_KEY);
			Number userLvlModelUsageMaxTokens = (Number) engineUserPermissionMap
					.get(Constants.USER_MODEL_MAX_TOKEN_KEY);
			Number userLvlModelUsageMaxResponseTime = (Number) engineUserPermissionMap
					.get(Constants.USER_MODEL_MAX_RESPONSE_TIME_KEY);

			String engineLvlModelUsageRestriction = (String) engineUserPermissionMap
					.get(Constants.ENGINE_USAGE_RESTRICTION_KEY);
			String engineLvlModelUsageFrequency = (String) engineUserPermissionMap
					.get(Constants.ENGINE_USAGE_FREQUENCY_KEY);
			Number engineLvlModelUsageMaxTokens = (Number) engineUserPermissionMap.get(Constants.ENGINE_MAX_TOKEN_KEY);
			Number engineLvlModelUsageMaxResponseTime = (Number) engineUserPermissionMap
					.get(Constants.ENGINE_MAX_RESPONSE_TIME_KEY);
			Number engineLvlMaxInputTokens = (Number) engineUserPermissionMap.get(Constants.ENGINE_MAX_INPUT_TOKEN_KEY);
			Number engineLvlMaxOutputTokens = (Number) engineUserPermissionMap.get(Constants.ENGINE_MAX_OUTPUT_TOKEN_KEY);

			if (engineLvlModelUsageRestriction == null || engineLvlModelUsageRestriction.trim().isEmpty()) {
				Map<String, Object> engineDefaultLimit = SecurityEntityDefaultTokenUtils.getEngineDefaultTokenLimit(engineId);
				if (engineDefaultLimit != null) {
					engineLvlModelUsageRestriction = (String) engineDefaultLimit.get("usageRestriction");
					engineLvlModelUsageFrequency = (String) engineDefaultLimit.get("usageFrequency");
					engineLvlModelUsageMaxTokens = (Number) engineDefaultLimit.get("maxTokens");
					engineLvlMaxInputTokens = (Number) engineDefaultLimit.get("maxInputTokens");
					engineLvlMaxOutputTokens = (Number) engineDefaultLimit.get("maxOutputTokens");
				}
			}

			ZonedDateTime currentDateTime = Utility.getCurrentZonedDateTimeUTC();

			// engine usage
			if (engineLvlModelUsageRestriction != null && !engineLvlModelUsageRestriction.isEmpty()) {
				if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE
						.equalsIgnoreCase(engineLvlModelUsageRestriction)) {
					// compute time restriction
					if (!Utility.isModelInferenceLogsEnabled()) {
						throw new IllegalArgumentException(
								"Model restrictions have been enabled but not properly configured on the platform. Please reach out to a system administrator");
					}
					Number currentUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
							Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE, user, engineId, currentDateTime,
							engineLvlModelUsageFrequency);

					if (engineLvlModelUsageMaxResponseTime != null
							&& currentUsage.doubleValue() > engineLvlModelUsageMaxResponseTime.doubleValue()) {
						throw new IllegalArgumentException(String.format(ENGINE_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE,
								currentUsage.doubleValue(), engineLvlModelUsageMaxResponseTime.doubleValue()));
					}

					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,
							Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE);
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
							currentUsage.intValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,
							engineLvlModelUsageMaxResponseTime != null ? engineLvlModelUsageMaxResponseTime.intValue() : 0);

				} else {
					// token-based restriction: check all defined limits in order combined, input, output
					checkEngineLevelTokenLimits(user, engineId, currentDateTime,
							engineLvlModelUsageFrequency, engineLvlModelUsageMaxTokens,
							engineLvlMaxInputTokens, engineLvlMaxOutputTokens, userRestrictionMap);
				}
			}

			// project-level restriction
			if (projectId != null && !projectId.trim().isEmpty()) {
				checkProjectLevelRestriction(user, engineId, projectId,
						Utility.getCurrentZonedDateTimeUTC(), userRestrictionMap);
			}

			// user general restriction (only if no engine-level token limits were applied)
			if (userRestrictionMap.isEmpty()
					&& userLvlModelUsageRestriction != null && !userLvlModelUsageRestriction.isEmpty()) {
				checkUserLevelRestriction(user, engineId, userLvlModelUsageRestriction, userLvlModelUsageFrequency,
						userLvlModelUsageMaxTokens, userLvlModelUsageMaxResponseTime, currentDateTime, userRestrictionMap);
			}
		}
		// If no engine permission row exists but we have a projectId, still check project-level
		else if (projectId != null && !projectId.trim().isEmpty()) {
			ZonedDateTime currentDateTime = Utility.getCurrentZonedDateTimeUTC();
			checkProjectLevelRestriction(user, engineId, projectId, currentDateTime, userRestrictionMap);
		}

		return userRestrictionMap;
	}

	/**
	 * check engine-level token limits in order of combined, input, output
	 */
	private static void checkEngineLevelTokenLimits(User user, String engineId,
			ZonedDateTime currentDateTime, String frequency,
			Number maxTokens, Number maxInputTokens, Number maxOutputTokens,
			Map<String, Object> userRestrictionMap) {

		boolean hasAnyLimit = (maxTokens != null && maxTokens.intValue() > 0)
				|| (maxInputTokens != null && maxInputTokens.intValue() > 0)
				|| (maxOutputTokens != null && maxOutputTokens.intValue() > 0);
		if (!hasAnyLimit) {
			return;
		}

		if (!Utility.isModelInferenceLogsEnabled()) {
			throw new IllegalArgumentException(
					"Model restrictions have been enabled but not properly configured on the platform. Please reach out to a system administrator");
		}

		userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,
				Constants.MODEL_TOKEN_RESTRICTION_VALUE);

		// combined token limit
		if (maxTokens != null && maxTokens.intValue() > 0) {
			Number combinedUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
					Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, engineId, currentDateTime, frequency);
			if (combinedUsage.intValue() > maxTokens.intValue()) {
				throw new IllegalArgumentException(String.format(ENGINE_TOKEN_LIMIT_EXCEEDED_MESSAGE,
						combinedUsage.intValue(), maxTokens.intValue()));
			}
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
					combinedUsage.intValue());
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,
					maxTokens.intValue());
		}

		// input token limit
		if (maxInputTokens != null && maxInputTokens.intValue() > 0) {
			Number inputUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
					Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, engineId, currentDateTime, frequency, "INPUT");
			if (inputUsage.intValue() > maxInputTokens.intValue()) {
				throw new IllegalArgumentException(String.format(ENGINE_INPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
						inputUsage.intValue(), maxInputTokens.intValue()));
			}
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_INPUT_CURRENT,
					inputUsage.intValue());
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_INPUT_MAX,
					maxInputTokens.intValue());
		}

		// output token limit
		if (maxOutputTokens != null && maxOutputTokens.intValue() > 0) {
			Number outputUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
					Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, engineId, currentDateTime, frequency, "RESPONSE");
			if (outputUsage.intValue() > maxOutputTokens.intValue()) {
				throw new IllegalArgumentException(String.format(ENGINE_OUTPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
						outputUsage.intValue(), maxOutputTokens.intValue()));
			}
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_OUTPUT_CURRENT,
					outputUsage.intValue());
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_OUTPUT_MAX,
					maxOutputTokens.intValue());
		}
	}

	/**
	 * Check project-level usage restrictions.
	 * Checks all defined token limits (combined → input → output) independently.
	 * For compute time, it is a separate mode.
	 */
	private static void checkProjectLevelRestriction(User user, String engineId, String projectId,
			ZonedDateTime currentDateTime, Map<String, Object> userRestrictionMap) {
		List<Map<String, Object>> projectPermission = SecurityProjectUtils.getProjectUsagePermissionMap(user, projectId);
		Map<String, Object> projMap = null;
		if (projectPermission != null && !projectPermission.isEmpty()) {
			projMap = projectPermission.get(0);
		}
		if (projMap == null) {
			projMap = SecurityEntityDefaultTokenUtils.getProjectDefaultTokenLimit(projectId);
			if (projMap == null) {
				return;
			}
		}
		String projRestriction = (String) projMap.get(Constants.PROJECT_USAGE_RESTRICTION_KEY);
		if (projRestriction == null) {
			projRestriction = (String) projMap.get("usageRestriction");
		}
		if (projRestriction == null || projRestriction.trim().isEmpty()) {
			Map<String, Object> projDefaultMap = SecurityEntityDefaultTokenUtils.getProjectDefaultTokenLimit(projectId);
			if (projDefaultMap == null) {
				return;
			}
			projMap = projDefaultMap;
			projRestriction = (String) projMap.get(Constants.PROJECT_USAGE_RESTRICTION_KEY);
			if (projRestriction == null) {
				projRestriction = (String) projMap.get("usageRestriction");
			}
			if (projRestriction == null || projRestriction.trim().isEmpty()) {
				return;
			}
		}

		if (!Utility.isModelInferenceLogsEnabled()) {
			throw new IllegalArgumentException(
					"Project model restrictions have been enabled but not properly configured on the platform. Please reach out to a system administrator");
		}

		String projFrequency = (String) projMap.get(Constants.PROJECT_USAGE_FREQUENCY_KEY);
		if (projFrequency == null) {
			projFrequency = (String) projMap.get("usageFrequency");
		}
		Number projMaxTokens = (Number) projMap.get(Constants.PROJECT_MAX_TOKEN_KEY);
		if (projMaxTokens == null) {
			projMaxTokens = (Number) projMap.get("maxTokens");
		}
		Number projMaxInputTokens = (Number) projMap.get(Constants.PROJECT_MAX_INPUT_TOKEN_KEY);
		if (projMaxInputTokens == null) {
			projMaxInputTokens = (Number) projMap.get("maxInputTokens");
		}
		Number projMaxOutputTokens = (Number) projMap.get(Constants.PROJECT_MAX_OUTPUT_TOKEN_KEY);
		if (projMaxOutputTokens == null) {
			projMaxOutputTokens = (Number) projMap.get("maxOutputTokens");
		}
		Number projMaxResponseTime = (Number) projMap.get(Constants.PROJECT_MAX_RESPONSE_TIME_KEY);
		if (projMaxResponseTime == null) {
			projMaxResponseTime = (Number) projMap.get("maxResponseTime");
		}
		Object restrictPerModelObj = projMap.get(Constants.PROJECT_RESTRICT_PER_MODEL_KEY);
		if (restrictPerModelObj == null) {
			restrictPerModelObj = projMap.get("restrictPerModel");
		}
		boolean restrictPerModel = restrictPerModelObj != null && Boolean.TRUE.equals(restrictPerModelObj);

		String scopedEngineId = restrictPerModel ? engineId : null;

		if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equalsIgnoreCase(projRestriction)) {
			Number computeUsage = ModelInferenceLogsUtils.getTotalTokensForProject(
					Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE, user, projectId, scopedEngineId, currentDateTime,
					projFrequency, null);
			if (projMaxResponseTime != null && computeUsage.doubleValue() > projMaxResponseTime.doubleValue()) {
				throw new IllegalArgumentException(String.format(PROJECT_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE,
						computeUsage.doubleValue(), projMaxResponseTime.doubleValue()));
			}
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_PROJECT_CURRENT, computeUsage.intValue());
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_PROJECT_MAX,
					projMaxResponseTime != null ? projMaxResponseTime.intValue() : 0);
		} else {
			// Token-based: check all defined limits independently
			boolean hasAnyLimit = (projMaxTokens != null && projMaxTokens.intValue() > 0)
					|| (projMaxInputTokens != null && projMaxInputTokens.intValue() > 0)
					|| (projMaxOutputTokens != null && projMaxOutputTokens.intValue() > 0);
			if (!hasAnyLimit) {
				return;
			}

			// combined token limit
			if (projMaxTokens != null && projMaxTokens.intValue() > 0) {
				Number combinedUsage = ModelInferenceLogsUtils.getTotalTokensForProject(
						Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, projectId, scopedEngineId, currentDateTime,
						projFrequency, null);
				if (combinedUsage.intValue() > projMaxTokens.intValue()) {
					throw new IllegalArgumentException(String.format(PROJECT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
							combinedUsage.intValue(), projMaxTokens.intValue()));
				}
				userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_PROJECT_CURRENT, combinedUsage.intValue());
				userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_PROJECT_MAX, projMaxTokens.intValue());
			}

			// input token limit
			if (projMaxInputTokens != null && projMaxInputTokens.intValue() > 0) {
				Number inputUsage = ModelInferenceLogsUtils.getTotalTokensForProject(
						Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, projectId, scopedEngineId, currentDateTime,
						projFrequency, "INPUT");
				if (inputUsage.intValue() > projMaxInputTokens.intValue()) {
					throw new IllegalArgumentException(String.format(PROJECT_INPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
							inputUsage.intValue(), projMaxInputTokens.intValue()));
				}
			}

			// output token limit
			if (projMaxOutputTokens != null && projMaxOutputTokens.intValue() > 0) {
				Number outputUsage = ModelInferenceLogsUtils.getTotalTokensForProject(
						Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, projectId, scopedEngineId, currentDateTime,
						projFrequency, "RESPONSE");
				if (outputUsage.intValue() > projMaxOutputTokens.intValue()) {
					throw new IllegalArgumentException(String.format(PROJECT_OUTPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
							outputUsage.intValue(), projMaxOutputTokens.intValue()));
				}
			}
		}
	}

	/**
	 * Check room-level token limits.
	 * Retrieves the effective limit for the user (user-specific or default) from ROOMTOKENLIMIT,
	 * then checks combined, input, and output token counts for this room.
	 */
	private static void checkRoomLevelRestriction(User user, String roomId,
			Map<String, Object> userRestrictionMap) {
		String userId = user.getAccessToken(user.getLogins().get(0)).getId();

		Map<String, Object> roomLimit = SecurityRoomTokenUtils.getEffectiveRoomTokenLimit(userId);
		if (roomLimit == null) {
			return;
		}
		Object isActiveObj = roomLimit.get("isActive");
		if (isActiveObj != null && !Boolean.TRUE.equals(isActiveObj)) {
			return;
		}

		Number maxTokens = (Number) roomLimit.get("maxTokens");
		Number maxInputTokens = (Number) roomLimit.get("maxInputTokens");
		Number maxOutputTokens = (Number) roomLimit.get("maxOutputTokens");

		boolean hasAnyLimit = (maxTokens != null && maxTokens.longValue() > 0)
				|| (maxInputTokens != null && maxInputTokens.longValue() > 0)
				|| (maxOutputTokens != null && maxOutputTokens.longValue() > 0);
		if (!hasAnyLimit) {
			return;
		}

		if (!Utility.isModelInferenceLogsEnabled()) {
			throw new IllegalArgumentException(
					"Room token restrictions have been enabled but inference logs are not configured on the platform. Please reach out to a system administrator");
		}

		// combined token limit
		if (maxTokens != null && maxTokens.longValue() > 0) {
			Number combinedUsage = ModelInferenceLogsUtils.getTotalTokensForRoom(roomId, null);
			if (combinedUsage.longValue() > maxTokens.longValue()) {
				throw new IllegalArgumentException(String.format(
						ROOM_TOKEN_LIMIT_EXCEEDED_MESSAGE,
						combinedUsage.longValue(), maxTokens.longValue()));
			}
			userRestrictionMap.put("roomTokensCurrent", combinedUsage.longValue());
			userRestrictionMap.put("roomTokensMax", maxTokens.longValue());
		}

		// input token limit
		if (maxInputTokens != null && maxInputTokens.longValue() > 0) {
			Number inputUsage = ModelInferenceLogsUtils.getTotalTokensForRoom(roomId, "INPUT");
			if (inputUsage.longValue() > maxInputTokens.longValue()) {
				throw new IllegalArgumentException(String.format(
						ROOM_INPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
						inputUsage.longValue(), maxInputTokens.longValue()));
			}
			userRestrictionMap.put("roomInputTokensCurrent", inputUsage.longValue());
			userRestrictionMap.put("roomInputTokensMax", maxInputTokens.longValue());
		}

		// output token limit
		if (maxOutputTokens != null && maxOutputTokens.longValue() > 0) {
			Number outputUsage = ModelInferenceLogsUtils.getTotalTokensForRoom(roomId, "RESPONSE");
			if (outputUsage.longValue() > maxOutputTokens.longValue()) {
				throw new IllegalArgumentException(String.format(
						ROOM_OUTPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
						outputUsage.longValue(), maxOutputTokens.longValue()));
			}
			userRestrictionMap.put("roomOutputTokensCurrent", outputUsage.longValue());
			userRestrictionMap.put("roomOutputTokensMax", maxOutputTokens.longValue());
		}
	}

	/**
	 * Check user-level global usage restrictions.
	 */
	private static void checkUserLevelRestriction(User user, String engineId,
			String userLvlModelUsageRestriction, String userLvlModelUsageFrequency,
			Number userLvlModelUsageMaxTokens, Number userLvlModelUsageMaxResponseTime,
			ZonedDateTime currentDateTime, Map<String, Object> userRestrictionMap) {
		if (!Utility.isModelInferenceLogsEnabled()) {
			throw new IllegalArgumentException(
					"User model restrictions have been enabled but not properly configured on the platform. Please reach out to a system administrator");
		}

		Number currentUsage = null;
		if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(userLvlModelUsageRestriction)) {
			currentUsage = ModelInferenceLogsUtils.getTotalUsageForUser(Constants.MODEL_TOKEN_RESTRICTION_VALUE,
					user, engineId, currentDateTime, userLvlModelUsageFrequency);

			if (currentUsage.intValue() > userLvlModelUsageMaxTokens.intValue()) {
				throw new IllegalArgumentException(String.format(USER_TOKEN_LIMIT_EXCEEDED_MESSAGE,
						currentUsage.intValue(), userLvlModelUsageMaxTokens.intValue()));
			}
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,
					Constants.MODEL_TOKEN_RESTRICTION_VALUE);
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
					currentUsage.intValue());
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,
					userLvlModelUsageMaxTokens.intValue());

		} else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE
				.equalsIgnoreCase(userLvlModelUsageRestriction)) {
			currentUsage = ModelInferenceLogsUtils.getTotalUsageForUser(
					Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE, user, engineId, currentDateTime,
					userLvlModelUsageFrequency);

			if (currentUsage.doubleValue() > userLvlModelUsageMaxResponseTime.doubleValue()) {
				throw new IllegalArgumentException(String.format(USER_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE,
						currentUsage.doubleValue(), userLvlModelUsageMaxResponseTime.doubleValue()));
			}
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,
					Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE);
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
					currentUsage.intValue());
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,
					userLvlModelUsageMaxResponseTime.intValue());

		} else {
			classLogger.warn("Unknown user level model restriction type = '" + userLvlModelUsageRestriction
					+ "' for user = " + User.getSingleLogginName(user));
		}
	}

	/**
	 * 
	 * @param userRestrictionMap
	 * @param modelResponse
	 * @param inputTime
	 * @param outputTime
	 */
	public static void updateRestrictionMapCurrentUsage(Map<String, Object> userRestrictionMap,
			AbstractModelEngineResponse<?> modelResponse, ZonedDateTime inputTime, ZonedDateTime outputTime) {
		if (userRestrictionMap != null && !userRestrictionMap.isEmpty()) {
			String restrictionMode = (String) userRestrictionMap
					.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE);

			if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(restrictionMode)) {
				userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
						// put in the new value of the current usage we calculated + the number of
						// tokens we just created
						((Number) userRestrictionMap.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE))
								.intValue() + modelResponse.getNumberOfTokensInPrompt()
								+ modelResponse.getNumberOfTokensInResponse());

			} else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equals(restrictionMode)) {

				Duration duration = Duration.between(inputTime, outputTime);
				long millisecondsDifference = duration.toMillis();
				Double millisecondsDouble = (double) millisecondsDifference;

				userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
						// put in the new value of the current usage we calculated + the time for this
						// new response
						((Number) userRestrictionMap.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE))
								.doubleValue() + millisecondsDouble);
			}

			// now add this to the model response
			modelResponse.setUsageRestriction(userRestrictionMap);
		}
	}

	/**
	 * Parse a frequency string and return the appropriate date range. Supports
	 * complex frequency specifications: - Simple: WEEK, MONTH, YEAR, ALL_TIME, DAY
	 *
	 * @param frequency       The frequency string
	 * @param currentDateTime The current date/time
	 * @return Map containing "start" and "end" ZonedDateTime values
	 */
	public static Map<String, ZonedDateTime> getDateRangeFromFrequency(String frequency,
			ZonedDateTime currentDateTime) {
		if (frequency == null || frequency.isEmpty()) {
			// Default to daily
			return getDayStartEndDate(currentDateTime);
		}

		String freq = frequency.toUpperCase().trim();

		// Handle simple calendar-based cases
		if (freq.equals("WEEK")) {
			return getWeekStartEndDate(currentDateTime);
		} else if (freq.equals("MONTH")) {
			return getMonthStartEndDate(currentDateTime);
		} else if (freq.equals("YEAR")) {
			return getYearStartEndDate(currentDateTime);
		} else if (freq.equals("ALL_TIME")) {
			return getEpochStartEndDate(currentDateTime);
		}

		// Default to daily
		return getDayStartEndDate(currentDateTime);
	}

	/**
	 * 
	 * @param utcDateTime
	 * @return the map containing start and end of the week.
	 */
	private static Map<String, ZonedDateTime> getWeekStartEndDate(ZonedDateTime utcDateTime) {
		Map<String, ZonedDateTime> weekDates = new HashMap<>();

		// Find the start of the week (Sunday)
		ZonedDateTime start = utcDateTime;
		while (start.getDayOfWeek() != DayOfWeek.SUNDAY) {
			start = start.minusDays(1);
		}

		// Find the end of the week (Saturday)
		ZonedDateTime end = utcDateTime;
		while (end.getDayOfWeek() != DayOfWeek.SATURDAY) {
			end = end.plusDays(1);
		}
		// Convert ZonedDateTime to LocalDateTime
		weekDates.put("start", start.toLocalDate().atStartOfDay(utcDateTime.getZone()));
		weekDates.put("end", end.toLocalDate().atTime(LocalTime.MAX).atZone(utcDateTime.getZone()));

		return weekDates;
	}

	/**
	 * 
	 * @param utcDateTime
	 * @return the map containing start and end of the month.
	 */
	private static Map<String, ZonedDateTime> getMonthStartEndDate(ZonedDateTime utcDateTime) {
		Map<String, ZonedDateTime> dates = new HashMap<>();
		// Find the start of the month by setting the day to 1.
		dates.put("start", utcDateTime.withDayOfMonth(1).toLocalDate().atStartOfDay(utcDateTime.getZone()));
		// Find the end of the month by setting the day to the last day of the month.
		dates.put("end", utcDateTime.withDayOfMonth(utcDateTime.toLocalDate().lengthOfMonth()).toLocalDate()
				.atTime(LocalTime.MAX).atZone(utcDateTime.getZone()));

		return dates;
	}

	/**
	 *
	 * @param utcDateTime
	 * @return the map containing start and end of the year.
	 */
	private static Map<String, ZonedDateTime> getYearStartEndDate(ZonedDateTime utcDateTime) {
		Map<String, ZonedDateTime> dates = new HashMap<>();
		// Find the start of the year by setting the day to January 1.
		dates.put("start", utcDateTime.withDayOfYear(1).toLocalDate().atStartOfDay(utcDateTime.getZone()));
		// Find the end of the year by setting the day to the last day of the year.
		dates.put("end", utcDateTime.withDayOfYear(utcDateTime.toLocalDate().lengthOfYear()).toLocalDate()
				.atTime(LocalTime.MAX).atZone(utcDateTime.getZone()));

		return dates;
	}

	/**
	 *
	 * @param utcDateTime
	 * @return the map containing start of epoch and current datetime.
	 */
	private static Map<String, ZonedDateTime> getEpochStartEndDate(ZonedDateTime utcDateTime) {
		Map<String, ZonedDateTime> dates = new HashMap<>();
		dates.put("start", ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));
		dates.put("end", utcDateTime);
		return dates;
	}

	/**
	 * Get start and end of the current day in UTC.
	 *
	 * @param utcDateTime The current date/time
	 * @return Map with start (00:00:00) and end (23:59:59.999999999) of the day
	 */
	private static Map<String, ZonedDateTime> getDayStartEndDate(ZonedDateTime utcDateTime) {
		Map<String, ZonedDateTime> dates = new HashMap<>();
		ZonedDateTime startOfDay = utcDateTime.toLocalDate().atStartOfDay(ZoneOffset.UTC);
		ZonedDateTime endOfDay = startOfDay.plusDays(1).minusNanos(1);
		dates.put("start", startOfDay);
		dates.put("end", endOfDay);
		return dates;
	}

	private ModelUsageRestrictionUtility() {

	}
}
