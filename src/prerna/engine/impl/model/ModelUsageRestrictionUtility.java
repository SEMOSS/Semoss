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

import prerna.auth.User;
import prerna.auth.utils.SecurityEntityDefaultTokenUtils;
import prerna.auth.utils.SecurityModelTokenUtils;
import prerna.auth.utils.SecurityPrincipalTokenLimitUtils;
import prerna.auth.utils.SecurityRoomTokenUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.util.Constants;
import prerna.util.Utility;

public final class ModelUsageRestrictionUtility {

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
	public static final String PLATFORM_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Token limit exceeded for model platform level: This model has used %d tokens, but the limit is %d";
	public static final String PLATFORM_INPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Input token limit exceeded for model platform level: This model has used %d input tokens, but the limit is %d";
	public static final String PLATFORM_OUTPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Output token limit exceeded for model platform level: This model has used %d output tokens, but the limit is %d";
	public static final String PLATFORM_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE = "Response time limit exceeded for model platform level: This model has reached %.2f seconds, but the limit is %.2f seconds.";
	public static final String TEAM_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Token limit exceeded for team level: You have used %d tokens, but the limit is %d";
	public static final String TEAM_INPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Input token limit exceeded for team level: You have used %d input tokens, but the limit is %d";
	public static final String TEAM_OUTPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Output token limit exceeded for team level: You have used %d output tokens, but the limit is %d";
	public static final String TEAM_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE = "Response time limit exceeded for team level: You have reached %.2f seconds, but the limit is %.2f seconds.";
	private static final String LIMIT_SCOPE_KEY = "__limitScope";
	private static final String LIMIT_SCOPE_USER = "USER";
	private static final String LIMIT_SCOPE_TEAM = "TEAM";

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
	 * Check all applicable usage restrictions for a user on a model engine in
	 * priority order of room policy, engine, project, user.
	 *
	 *
	 * @param user      the requesting user
	 * @param engineId  the model engine id
	 * @param projectId optional project id for project-level checks
	 * @param roomId    optional room id for room-usage checks against the platform
	 *                  room policy
	 * @return map with restriction mode and current/max values for the response payload
	 */
	public static Map<String, Object> getModelUsageRestriction(User user, String engineId, String projectId, String roomId) {
		Map<String, Object> userRestrictionMap = new HashMap<>();
		ZonedDateTime currentDateTime = Utility.getCurrentZonedDateTimeUTC();

		// platform room-policy check against this room's usage
		if (roomId != null && !roomId.trim().isEmpty()) {
			checkRoomLevelRestriction(user, roomId, userRestrictionMap);
		}

		checkModelPlatformRestriction(engineId, currentDateTime);
		checkEngineLevelRestriction(user, engineId, currentDateTime, userRestrictionMap);

		if (projectId != null && !projectId.trim().isEmpty()) {
			checkProjectLevelRestriction(user, engineId, projectId, currentDateTime, userRestrictionMap);
		}

		return userRestrictionMap;
	}

	private static void checkModelPlatformRestriction(String engineId, ZonedDateTime currentDateTime) {
		List<Map<String, Object>> platformLimits = SecurityModelTokenUtils.getModelTokenLimits(engineId);
		if (platformLimits == null || platformLimits.isEmpty()) {
			return;
		}

		for (Map<String, Object> limit : platformLimits) {
			if (!isActive(limit.get("isActive"))) {
				continue;
			}

			String frequency = (String) limit.get("usageFrequency");
			Number maxTokens = (Number) limit.get("maxTokens");
			Number maxInputTokens = (Number) limit.get("maxInputTokens");
			Number maxOutputTokens = (Number) limit.get("maxOutputTokens");
			Number maxResponseTime = (Number) limit.get("maxResponseTime");

			if (!hasConfiguredLimit(Constants.MODEL_TOKEN_RESTRICTION_VALUE, maxTokens, maxInputTokens, maxOutputTokens,
					maxResponseTime)) {
				continue;
			}

			if (!Utility.isModelInferenceLogsEnabled()) {
				throw new IllegalArgumentException(
						"Model restrictions have been enabled but not properly configured on the platform. Please reach out to a system administrator");
			}

			if (maxTokens != null && maxTokens.longValue() >= 0) {
				Number combinedUsage = ModelInferenceLogsUtils.getTotalTokensForEngine(
						Constants.MODEL_TOKEN_RESTRICTION_VALUE, engineId, currentDateTime, frequency, null);
				if (combinedUsage.longValue() > maxTokens.longValue()) {
					throw new IllegalArgumentException(String.format(PLATFORM_TOKEN_LIMIT_EXCEEDED_MESSAGE,
							combinedUsage.longValue(), maxTokens.longValue()));
				}
			}
			if (maxInputTokens != null && maxInputTokens.longValue() >= 0) {
				Number inputUsage = ModelInferenceLogsUtils.getTotalTokensForEngine(
						Constants.MODEL_TOKEN_RESTRICTION_VALUE, engineId, currentDateTime, frequency, "INPUT");
				if (inputUsage.longValue() > maxInputTokens.longValue()) {
					throw new IllegalArgumentException(String.format(PLATFORM_INPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
							inputUsage.longValue(), maxInputTokens.longValue()));
				}
			}
			if (maxOutputTokens != null && maxOutputTokens.longValue() >= 0) {
				Number outputUsage = ModelInferenceLogsUtils.getTotalTokensForEngine(
						Constants.MODEL_TOKEN_RESTRICTION_VALUE, engineId, currentDateTime, frequency, "RESPONSE");
				if (outputUsage.longValue() > maxOutputTokens.longValue()) {
					throw new IllegalArgumentException(String.format(PLATFORM_OUTPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
							outputUsage.longValue(), maxOutputTokens.longValue()));
				}
			}
			if (maxResponseTime != null && maxResponseTime.doubleValue() >= 0) {
				Number computeUsage = ModelInferenceLogsUtils.getTotalTokensForEngine(
						Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE, engineId, currentDateTime, frequency, null);
				if (computeUsage.doubleValue() > maxResponseTime.doubleValue()) {
					throw new IllegalArgumentException(String.format(PLATFORM_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE,
							computeUsage.doubleValue(), maxResponseTime.doubleValue()));
				}
			}
		}
	}

	private static void checkEngineLevelRestriction(User user, String engineId, ZonedDateTime currentDateTime,
			Map<String, Object> userRestrictionMap) {
		List<Map<String, Object>> engineUserTokenLimits = SecurityPrincipalTokenLimitUtils
				.getApplicableEngineUserTokenLimits(user, engineId);
		List<Map<String, Object>> specificUserCandidates = new java.util.ArrayList<>();
		for (Map<String, Object> limit : engineUserTokenLimits) {
			if (!hasActiveConfiguredPrincipalLimit(limit)) {
				continue;
			}
			specificUserCandidates.add(markLimitScope(limit, LIMIT_SCOPE_USER));
		}

		List<Map<String, Object>> engineTeamTokenLimits = SecurityPrincipalTokenLimitUtils
				.getApplicableEngineTeamTokenLimits(user, engineId);
		List<Map<String, Object>> specificTeamCandidates = new java.util.ArrayList<>();
		for (Map<String, Object> limit : engineTeamTokenLimits) {
			if (!hasActiveConfiguredPrincipalLimit(limit)) {
				continue;
			}
			specificTeamCandidates.add(markLimitScope(limit, LIMIT_SCOPE_TEAM));
		}

		List<Map<String, Object>> defaultUserLimits = SecurityEntityDefaultTokenUtils.getEngineDefaultTokenLimits(engineId);
		List<Map<String, Object>> defaultUserCandidates = new java.util.ArrayList<>();
		for (Map<String, Object> defaultUserLimit : defaultUserLimits) {
			if (!hasActiveConfiguredDefaultLimit(defaultUserLimit)) {
				continue;
			}
			defaultUserCandidates.add(markLimitScope(defaultUserLimit, LIMIT_SCOPE_USER));
		}

		List<Map<String, Object>> defaultTeamLimits = SecurityEntityDefaultTokenUtils.getEngineDefaultTeamTokenLimits(engineId);
		List<Map<String, Object>> defaultTeamCandidates = new java.util.ArrayList<>();
		for (Map<String, Object> defaultTeamLimit : defaultTeamLimits) {
			if (!hasActiveConfiguredDefaultLimit(defaultTeamLimit)) {
				continue;
			}
			defaultTeamCandidates.add(markLimitScope(defaultTeamLimit, LIMIT_SCOPE_TEAM));
		}

		Map<String, Map<String, Object>> selectedByFrequency = selectEffectiveLimitsByFrequency(
				specificUserCandidates, specificTeamCandidates, defaultUserCandidates, defaultTeamCandidates);
		for (Map<String, Object> selectedLimit : selectedByFrequency.values()) {
			boolean fromTeam = isTeamScopedLimit(selectedLimit);
			applyEngineRestriction(user, engineId, currentDateTime, (String) selectedLimit.get("usageRestriction"),
					(String) selectedLimit.get("usageFrequency"), (Number) selectedLimit.get("maxTokens"),
					(Number) selectedLimit.get("maxInputTokens"), (Number) selectedLimit.get("maxOutputTokens"),
					(Number) selectedLimit.get("maxResponseTime"), userRestrictionMap,
					fromTeam ? TEAM_TOKEN_LIMIT_EXCEEDED_MESSAGE : ENGINE_TOKEN_LIMIT_EXCEEDED_MESSAGE,
					fromTeam ? TEAM_INPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE : ENGINE_INPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
					fromTeam ? TEAM_OUTPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE : ENGINE_OUTPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
					fromTeam ? TEAM_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE : ENGINE_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE);
		}
	}

	private static void applyEngineRestriction(User user, String engineId, ZonedDateTime currentDateTime,
			String restriction, String frequency, Number maxTokens, Number maxInputTokens, Number maxOutputTokens,
			Number maxResponseTime, Map<String, Object> userRestrictionMap, String tokenExceededMessage,
			String inputExceededMessage, String outputExceededMessage, String responseTimeExceededMessage) {
		if (!Utility.isModelInferenceLogsEnabled()) {
			throw new IllegalArgumentException(
					"Model restrictions have been enabled but not properly configured on the platform. Please reach out to a system administrator");
		}

		if (maxResponseTime != null && maxResponseTime.doubleValue() >= 0) {
			Number currentUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
					Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE, user, engineId, currentDateTime, frequency);
			if (maxResponseTime != null && currentUsage.doubleValue() > maxResponseTime.doubleValue()) {
				throw new IllegalArgumentException(String.format(responseTimeExceededMessage, currentUsage.doubleValue(),
						maxResponseTime.doubleValue()));
			}

			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,
					Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE);
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
					currentUsage.intValue());
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,
					maxResponseTime != null ? maxResponseTime.intValue() : 0);
		}

		checkEngineLevelTokenLimits(user, engineId, currentDateTime, frequency, maxTokens, maxInputTokens,
				maxOutputTokens, userRestrictionMap, tokenExceededMessage, inputExceededMessage,
				outputExceededMessage);
	}

	private static void checkEngineLevelTokenLimits(User user, String engineId, ZonedDateTime currentDateTime,
			String frequency, Number maxTokens, Number maxInputTokens, Number maxOutputTokens,
			Map<String, Object> userRestrictionMap, String tokenExceededMessage, String inputExceededMessage,
			String outputExceededMessage) {
		Number normalizedMaxInputTokens = maxTokens != null && maxTokens.longValue() >= 0 ? null : maxInputTokens;
		Number normalizedMaxOutputTokens = maxTokens != null && maxTokens.longValue() >= 0 ? null : maxOutputTokens;

		boolean hasAnyLimit = (maxTokens != null && maxTokens.longValue() >= 0)
				|| (normalizedMaxInputTokens != null && normalizedMaxInputTokens.longValue() >= 0)
				|| (normalizedMaxOutputTokens != null && normalizedMaxOutputTokens.longValue() >= 0);
		if (!hasAnyLimit) {
			return;
		}

		if (!Utility.isModelInferenceLogsEnabled()) {
			throw new IllegalArgumentException(
					"Model restrictions have been enabled but not properly configured on the platform. Please reach out to a system administrator");
		}

		userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,
				Constants.MODEL_TOKEN_RESTRICTION_VALUE);

		if (maxTokens != null && maxTokens.longValue() >= 0) {
			Number combinedUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
					Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, engineId, currentDateTime, frequency);
			if (combinedUsage.longValue() > maxTokens.longValue()) {
				throw new IllegalArgumentException(
						String.format(tokenExceededMessage, combinedUsage.longValue(), maxTokens.longValue()));
			}
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
					combinedUsage.intValue());
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE, maxTokens.intValue());
		}

		if (normalizedMaxInputTokens != null && normalizedMaxInputTokens.longValue() >= 0) {
			Number inputUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
					Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, engineId, currentDateTime, frequency, "INPUT");
			if (inputUsage.longValue() > normalizedMaxInputTokens.longValue()) {
				throw new IllegalArgumentException(
						String.format(inputExceededMessage, inputUsage.longValue(),
								normalizedMaxInputTokens.longValue()));
			}
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_INPUT_CURRENT, inputUsage.intValue());
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_INPUT_MAX,
					normalizedMaxInputTokens.intValue());
		}

		if (normalizedMaxOutputTokens != null && normalizedMaxOutputTokens.longValue() >= 0) {
			Number outputUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
					Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, engineId, currentDateTime, frequency, "RESPONSE");
			if (outputUsage.longValue() > normalizedMaxOutputTokens.longValue()) {
				throw new IllegalArgumentException(
						String.format(outputExceededMessage, outputUsage.longValue(),
								normalizedMaxOutputTokens.longValue()));
			}
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_OUTPUT_CURRENT,
					outputUsage.intValue());
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_OUTPUT_MAX,
					normalizedMaxOutputTokens.intValue());
		}
	}

	private static void checkProjectLevelRestriction(User user, String engineId, String projectId,
			ZonedDateTime currentDateTime, Map<String, Object> userRestrictionMap) {
		List<Map<String, Object>> projectUserTokenLimits = SecurityPrincipalTokenLimitUtils
				.getApplicableProjectUserTokenLimits(user, projectId, engineId);
		List<Map<String, Object>> specificUserCandidates = new java.util.ArrayList<>();
		for (Map<String, Object> limit : projectUserTokenLimits) {
			if (!hasActiveConfiguredPrincipalLimit(limit)) {
				continue;
			}
			specificUserCandidates.add(markLimitScope(limit, LIMIT_SCOPE_USER));
		}

		List<Map<String, Object>> projectTeamTokenLimits = SecurityPrincipalTokenLimitUtils
				.getApplicableProjectTeamTokenLimits(user, projectId, engineId);
		List<Map<String, Object>> specificTeamCandidates = new java.util.ArrayList<>();
		for (Map<String, Object> limit : projectTeamTokenLimits) {
			if (!hasActiveConfiguredPrincipalLimit(limit)) {
				continue;
			}
			specificTeamCandidates.add(markLimitScope(limit, LIMIT_SCOPE_TEAM));
		}

		List<Map<String, Object>> defaultUserLimits = SecurityEntityDefaultTokenUtils.getProjectDefaultTokenLimits(projectId);
		List<Map<String, Object>> defaultUserCandidates = new java.util.ArrayList<>();
		for (Map<String, Object> defaultUserLimit : defaultUserLimits) {
			if (!hasActiveConfiguredDefaultLimit(defaultUserLimit)) {
				continue;
			}
			defaultUserCandidates.add(markLimitScope(defaultUserLimit, LIMIT_SCOPE_USER));
		}

		List<Map<String, Object>> defaultTeamLimits = SecurityEntityDefaultTokenUtils.getProjectDefaultTeamTokenLimits(projectId);
		List<Map<String, Object>> defaultTeamCandidates = new java.util.ArrayList<>();
		for (Map<String, Object> defaultTeamLimit : defaultTeamLimits) {
			if (!hasActiveConfiguredDefaultLimit(defaultTeamLimit)) {
				continue;
			}
			defaultTeamCandidates.add(markLimitScope(defaultTeamLimit, LIMIT_SCOPE_TEAM));
		}

		Map<String, Map<String, Object>> selectedByFrequency = selectEffectiveLimitsByFrequency(
				specificUserCandidates, specificTeamCandidates, defaultUserCandidates, defaultTeamCandidates);
		for (Map<String, Object> selectedLimit : selectedByFrequency.values()) {
			boolean fromTeam = isTeamScopedLimit(selectedLimit);
			applyProjectRestriction(user, engineId, projectId, currentDateTime,
					(String) selectedLimit.get("usageRestriction"), (String) selectedLimit.get("usageFrequency"),
					(Number) selectedLimit.get("maxTokens"), (Number) selectedLimit.get("maxInputTokens"),
					(Number) selectedLimit.get("maxOutputTokens"), (Number) selectedLimit.get("maxResponseTime"),
					isRestrictPerModel(selectedLimit, engineId), userRestrictionMap,
					fromTeam ? TEAM_TOKEN_LIMIT_EXCEEDED_MESSAGE : PROJECT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
					fromTeam ? TEAM_INPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE : PROJECT_INPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
					fromTeam ? TEAM_OUTPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE : PROJECT_OUTPUT_TOKEN_LIMIT_EXCEEDED_MESSAGE,
					fromTeam ? TEAM_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE : PROJECT_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE);
		}
	}

	private static void applyProjectRestriction(User user, String engineId, String projectId,
			ZonedDateTime currentDateTime, String restriction, String frequency, Number maxTokens,
			Number maxInputTokens, Number maxOutputTokens, Number maxResponseTime, boolean restrictPerModel,
			Map<String, Object> userRestrictionMap, String tokenExceededMessage, String inputExceededMessage,
			String outputExceededMessage, String responseTimeExceededMessage) {
		if (!Utility.isModelInferenceLogsEnabled()) {
			throw new IllegalArgumentException(
					"Project model restrictions have been enabled but not properly configured on the platform. Please reach out to a system administrator");
		}

		String scopedEngineId = restrictPerModel ? engineId : null;
		if (maxResponseTime != null && maxResponseTime.doubleValue() >= 0) {
			Number computeUsage = ModelInferenceLogsUtils.getTotalTokensForProject(
					Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE, user, projectId, scopedEngineId, currentDateTime,
					frequency, null);
			if (maxResponseTime != null && computeUsage.doubleValue() > maxResponseTime.doubleValue()) {
				throw new IllegalArgumentException(String.format(responseTimeExceededMessage, computeUsage.doubleValue(),
						maxResponseTime.doubleValue()));
			}
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_PROJECT_CURRENT,
					computeUsage.intValue());
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_PROJECT_MAX,
					maxResponseTime != null ? maxResponseTime.intValue() : 0);
		}

		checkProjectTokenLimits(user, projectId, scopedEngineId, currentDateTime, frequency, maxTokens,
				maxInputTokens, maxOutputTokens, userRestrictionMap, tokenExceededMessage, inputExceededMessage,
				outputExceededMessage);
	}

	private static void checkProjectTokenLimits(User user, String projectId, String scopedEngineId,
			ZonedDateTime currentDateTime, String frequency, Number maxTokens, Number maxInputTokens,
			Number maxOutputTokens, Map<String, Object> userRestrictionMap, String tokenExceededMessage,
			String inputExceededMessage, String outputExceededMessage) {
		Number normalizedMaxInputTokens = maxTokens != null && maxTokens.longValue() >= 0 ? null : maxInputTokens;
		Number normalizedMaxOutputTokens = maxTokens != null && maxTokens.longValue() >= 0 ? null : maxOutputTokens;
		boolean hasAnyLimit = (maxTokens != null && maxTokens.longValue() >= 0)
				|| (normalizedMaxInputTokens != null && normalizedMaxInputTokens.longValue() >= 0)
				|| (normalizedMaxOutputTokens != null && normalizedMaxOutputTokens.longValue() >= 0);
		if (!hasAnyLimit) {
			return;
		}

		if (maxTokens != null && maxTokens.longValue() >= 0) {
			Number combinedUsage = ModelInferenceLogsUtils.getTotalTokensForProject(
					Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, projectId, scopedEngineId, currentDateTime,
					frequency, null);
			if (combinedUsage.longValue() > maxTokens.longValue()) {
				throw new IllegalArgumentException(
						String.format(tokenExceededMessage, combinedUsage.longValue(), maxTokens.longValue()));
			}
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_PROJECT_CURRENT,
					combinedUsage.intValue());
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_PROJECT_MAX, maxTokens.intValue());
		}

		if (normalizedMaxInputTokens != null && normalizedMaxInputTokens.longValue() >= 0) {
			Number inputUsage = ModelInferenceLogsUtils.getTotalTokensForProject(
					Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, projectId, scopedEngineId, currentDateTime,
					frequency, "INPUT");
			if (inputUsage.longValue() > normalizedMaxInputTokens.longValue()) {
				throw new IllegalArgumentException(
						String.format(inputExceededMessage, inputUsage.longValue(),
								normalizedMaxInputTokens.longValue()));
			}
		}

		if (normalizedMaxOutputTokens != null && normalizedMaxOutputTokens.longValue() >= 0) {
			Number outputUsage = ModelInferenceLogsUtils.getTotalTokensForProject(
					Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, projectId, scopedEngineId, currentDateTime,
					frequency, "RESPONSE");
			if (outputUsage.longValue() > normalizedMaxOutputTokens.longValue()) {
				throw new IllegalArgumentException(
						String.format(outputExceededMessage, outputUsage.longValue(),
								normalizedMaxOutputTokens.longValue()));
			}
		}
	}

	private static Map<String, Map<String, Object>> selectEffectiveLimitsByFrequency(
			List<Map<String, Object>> specificUserCandidates, List<Map<String, Object>> specificTeamCandidates,
			List<Map<String, Object>> defaultUserCandidates, List<Map<String, Object>> defaultTeamCandidates) {
		Map<String, Map<String, Object>> selectedByFrequency = new HashMap<>();
		Map<String, Map<String, Object>> specificUserByFrequency = selectBestTokenRowsPerFrequency(specificUserCandidates);
		Map<String, Map<String, Object>> specificTeamByFrequency = selectBestTokenRowsPerFrequency(specificTeamCandidates);
		Map<String, Map<String, Object>> defaultUserByFrequency = selectBestTokenRowsPerFrequency(defaultUserCandidates);
		Map<String, Map<String, Object>> defaultTeamByFrequency = selectBestTokenRowsPerFrequency(defaultTeamCandidates);

		java.util.Set<String> frequencies = new java.util.HashSet<>();
		frequencies.addAll(specificUserByFrequency.keySet());
		frequencies.addAll(specificTeamByFrequency.keySet());
		frequencies.addAll(defaultUserByFrequency.keySet());
		frequencies.addAll(defaultTeamByFrequency.keySet());

		for (String frequency : frequencies) {
			Map<String, Object> userLimit = specificUserByFrequency.containsKey(frequency)
					? specificUserByFrequency.get(frequency)
					: defaultUserByFrequency.get(frequency);
			Map<String, Object> teamLimit = specificTeamByFrequency.containsKey(frequency)
					? specificTeamByFrequency.get(frequency)
					: defaultTeamByFrequency.get(frequency);
			Map<String, Object> selectedLimit = pickMorePermissiveTokenRow(userLimit, teamLimit);
			if (selectedLimit != null) {
				selectedByFrequency.put(frequency, selectedLimit);
			}
		}
		return selectedByFrequency;
	}

	private static Map<String, Map<String, Object>> selectBestTokenRowsPerFrequency(List<Map<String, Object>> limits) {
		Map<String, Map<String, Object>> bestByFrequency = new HashMap<>();
		for (Map<String, Object> limit : limits) {
			String frequency = (String) limit.get("usageFrequency");
			if (frequency == null || frequency.trim().isEmpty()) {
				continue;
			}
			Map<String, Object> currentBest = bestByFrequency.get(frequency);
			if (currentBest == null) {
				bestByFrequency.put(frequency, limit);
			} else {
				bestByFrequency.put(frequency, pickMorePermissiveTokenRow(currentBest, limit));
			}
		}
		return bestByFrequency;
	}

	private static Map<String, Object> pickMorePermissiveTokenRow(Map<String, Object> left, Map<String, Object> right) {
		if (left == null) {
			return right;
		}
		if (right == null) {
			return left;
		}
		int comparison = compareNullableLimitNumbers((Number) left.get("maxTokens"), (Number) right.get("maxTokens"));
		if (comparison != 0) {
			return comparison >= 0 ? left : right;
		}
		comparison = compareNullableLimitNumbers((Number) left.get("maxInputTokens"),
				(Number) right.get("maxInputTokens"));
		if (comparison != 0) {
			return comparison >= 0 ? left : right;
		}
		comparison = compareNullableLimitNumbers((Number) left.get("maxOutputTokens"),
				(Number) right.get("maxOutputTokens"));
		if (comparison != 0) {
			return comparison >= 0 ? left : right;
		}
		comparison = compareNullableLimitNumbers((Number) left.get("maxResponseTime"),
				(Number) right.get("maxResponseTime"));
		if (comparison != 0) {
			return comparison >= 0 ? left : right;
		}
		return left;
	}

	private static int compareNullableLimitNumbers(Number left, Number right) {
		boolean leftConfigured = left != null && left.doubleValue() >= 0;
		boolean rightConfigured = right != null && right.doubleValue() >= 0;
		if (leftConfigured && rightConfigured) {
			return Double.compare(left.doubleValue(), right.doubleValue());
		}
		if (leftConfigured) {
			return 1;
		}
		if (rightConfigured) {
			return -1;
		}
		return 0;
	}

	private static boolean isTeamScopedLimit(Map<String, Object> limit) {
		return LIMIT_SCOPE_TEAM.equals(limit.get(LIMIT_SCOPE_KEY))
				|| limit.get("groupId") != null || limit.get("groupType") != null;
	}

	private static Map<String, Object> markLimitScope(Map<String, Object> limit, String scope) {
		Map<String, Object> scopedLimit = new HashMap<>(limit);
		scopedLimit.put(LIMIT_SCOPE_KEY, scope);
		return scopedLimit;
	}

	private static boolean hasActiveConfiguredDefaultLimit(Map<String, Object> limitMap) {
		if (limitMap == null || !isActive(limitMap.get("isActive"))) {
			return false;
		}
		return hasConfiguredLimit((String) limitMap.get("usageRestriction"), (Number) limitMap.get("maxTokens"),
				(Number) limitMap.get("maxInputTokens"), (Number) limitMap.get("maxOutputTokens"),
				(Number) limitMap.get("maxResponseTime"));
	}

	private static boolean hasActiveConfiguredPrincipalLimit(Map<String, Object> limitMap) {
		if (limitMap == null || !isActive(limitMap.get("isActive"))) {
			return false;
		}
		return hasConfiguredLimit((String) limitMap.get("usageRestriction"), (Number) limitMap.get("maxTokens"),
				(Number) limitMap.get("maxInputTokens"), (Number) limitMap.get("maxOutputTokens"),
				(Number) limitMap.get("maxResponseTime"));
	}

	private static boolean isRestrictPerModel(Map<String, Object> limitMap, String engineId) {
		Object scopedEngineId = limitMap.get("engineId");
		if (scopedEngineId != null && engineId != null && engineId.equals(scopedEngineId.toString())) {
			return true;
		}
		return Boolean.TRUE.equals(limitMap.get("restrictPerModel"));
	}

	private static boolean hasConfiguredLimit(String restriction, Number maxTokens, Number maxInputTokens,
			Number maxOutputTokens, Number maxResponseTime) {
		if (restriction == null || restriction.trim().isEmpty()) {
			return false;
		}
		return (maxTokens != null && maxTokens.longValue() >= 0)
				|| (maxInputTokens != null && maxInputTokens.longValue() >= 0)
				|| (maxOutputTokens != null && maxOutputTokens.longValue() >= 0)
				|| (maxResponseTime != null && maxResponseTime.doubleValue() >= 0);
	}

	private static boolean isActive(Object isActiveObj) {
		return isActiveObj == null || Boolean.TRUE.equals(isActiveObj);
	}

	/**
	 * Check the platform room-token policy for this room.
	 * Retrieves the effective limit for the user (user-specific override or
	 * platform default) from ROOMTOKENLIMIT, then checks combined, input, and
	 * output token counts for this room.
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
				Number currentValue = (Number) userRestrictionMap
						.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE);
				if (currentValue != null) {
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
							currentValue.intValue() + safeTokenCount(modelResponse.getNumberOfTokensInPrompt())
									+ safeTokenCount(modelResponse.getNumberOfTokensInResponse()));
				}

				Number inputCurrentValue = (Number) userRestrictionMap
						.get(AbstractModelEngineResponse.USAGE_RESTRICTION_INPUT_CURRENT);
				if (inputCurrentValue != null) {
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_INPUT_CURRENT,
							inputCurrentValue.intValue() + safeTokenCount(modelResponse.getNumberOfTokensInPrompt()));
				}

				Number outputCurrentValue = (Number) userRestrictionMap
						.get(AbstractModelEngineResponse.USAGE_RESTRICTION_OUTPUT_CURRENT);
				if (outputCurrentValue != null) {
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_OUTPUT_CURRENT,
							outputCurrentValue.intValue() + safeTokenCount(modelResponse.getNumberOfTokensInResponse()));
				}

			} else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equals(restrictionMode)) {

				Duration duration = Duration.between(inputTime, outputTime);
				long millisecondsDifference = duration.toMillis();
				Double millisecondsDouble = (double) millisecondsDifference;

				Number currentValue = (Number) userRestrictionMap
						.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE);
				userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
						// put in the new value of the current usage we calculated + the time for this
						// new response
						(currentValue == null ? 0.0 : currentValue.doubleValue()) + millisecondsDouble);
			}

			// now add this to the model response
			modelResponse.setUsageRestriction(userRestrictionMap);
		}
	}

	private static int safeTokenCount(Integer tokenCount) {
		return tokenCount == null ? 0 : tokenCount.intValue();
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
		if (freq.equals("HOUR")) {
			return getHourStartEndDate(currentDateTime);
		} else if (freq.equals("DAY")) {
			return getDayStartEndDate(currentDateTime);
		} else if (freq.equals("WEEK")) {
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
	 * Get start and end of the current hour in UTC.
	 *
	 * @param utcDateTime The current date/time
	 * @return Map with start and end of the current hour
	 */
	private static Map<String, ZonedDateTime> getHourStartEndDate(ZonedDateTime utcDateTime) {
		Map<String, ZonedDateTime> dates = new HashMap<>();
		ZonedDateTime startOfHour = utcDateTime.withMinute(0).withSecond(0).withNano(0);
		ZonedDateTime endOfHour = startOfHour.plusHours(1).minusNanos(1);
		dates.put("start", startOfHour);
		dates.put("end", endOfHour);
		return dates;
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
