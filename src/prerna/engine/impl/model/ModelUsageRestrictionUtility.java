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
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

	// exception Message for throttle limit
	public static final String USER_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Token limit exceeded for user level : You have used %d tokens, but the limit is %d";
	public static final String USER_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE = "Response time limit exceeded for user level : You have reached %.2f seconds, but the limit is %.2f seconds.";
	public static final String USER_CREDIT_LIMIT_EXCEEDED_MESSAGE = "Credit limit exceeded for user level: You have used %.2f credits, but the limit is %.2f";
	public static final String ENGINE_TOKEN_LIMIT_EXCEEDED_MESSAGE = "Token limit exceeded for engine level: You have used %d tokens, but the limit is %d";
	public static final String ENGINE_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE = "Response time limit exceeded for engine level : You have reached %.2f seconds, but the limit is %.2f seconds.";
	public static final String ENGINE_CREDIT_LIMIT_EXCEEDED_MESSAGE = "Credit limit exceeded for engine level: You have used %.2f credits, but the limit is %.2f";

	/**
	 * 
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static Map<String, Object> getModelUsageRestriction(User user, String engineId) {
		Map<String, Object> userRestrictionMap = new LinkedHashMap<>();

		List<Map<String, Object>> engineUserPermission = SecurityEngineUtils.getEngineUsagePermissionMap(user, engineId);
		if (engineUserPermission == null || engineUserPermission.isEmpty()) {
			return userRestrictionMap;
		}
		Map<String, Object> engineUserPermissionMap = engineUserPermission.get(0);

		// Individual engine-level fields (engine-scoped usage)
		String engineLvlRestriction      = (String) engineUserPermissionMap.get(Constants.ENGINE_USAGE_RESTRICTION_KEY);
		String engineLvlFrequency        = (String) engineUserPermissionMap.get(Constants.ENGINE_USAGE_FREQUENCY_KEY);
		Number engineLvlMaxTokens        = (Number) engineUserPermissionMap.get(Constants.ENGINE_MAX_TOKEN_KEY);
		Number engineLvlMaxResponseTime  = (Number) engineUserPermissionMap.get(Constants.ENGINE_MAX_RESPONSE_TIME_KEY);
		Number engineLvlMaxCredits       = (Number) engineUserPermissionMap.get(Constants.ENGINE_MAX_CREDIT_KEY);

		// Individual user-level fields (cross-engine usage — always additive)
		String userLvlRestriction        = (String) engineUserPermissionMap.get(Constants.USER_USAGE_RESTRICTION_KEY);
		String userLvlFrequency          = (String) engineUserPermissionMap.get(Constants.USER_MODEL_USAGE_FREQUENCY_KEY);
		Number userLvlMaxTokens          = (Number) engineUserPermissionMap.get(Constants.USER_MODEL_MAX_TOKEN_KEY);
		Number userLvlMaxResponseTime    = (Number) engineUserPermissionMap.get(Constants.USER_MODEL_MAX_RESPONSE_TIME_KEY);
		Number userLvlMaxCredits         = (Number) engineUserPermissionMap.get(Constants.USER_MODEL_MAX_CREDIT_KEY);

		boolean anyIndividualRestriction = (engineLvlRestriction != null && !engineLvlRestriction.isEmpty())
				|| (userLvlRestriction != null && !userLvlRestriction.isEmpty());

		if (!Utility.isModelInferenceLogsEnabled()) {
			if (anyIndividualRestriction) {
				throw new IllegalArgumentException(
						"Model restrictions have been enabled but not properly configured on the platform. Please reach out to a system administrator");
			}
			return userRestrictionMap;
		}

		ZonedDateTime currentDateTime = Utility.getCurrentZonedDateTimeUTC();

		// === ENGINE-SCOPE BUCKET MAP ===
		// Merge individual engine restriction + all group restrictions by (type, frequency).
		// Within the same bucket, keep the MAX (most permissive) limit — memberships are additive.
		// Different (type, frequency) pairs are independent constraints that must all pass.
		Map<String, Map<String, Object>> engineScopeBuckets = new HashMap<>();

		if (engineLvlRestriction != null && !engineLvlRestriction.isEmpty()) {
			Number limit = null;
			if      (Constants.MODEL_CREDIT_RESTRICTION_VALUE.equalsIgnoreCase(engineLvlRestriction))       limit = engineLvlMaxCredits;
			else if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(engineLvlRestriction))        limit = engineLvlMaxTokens;
			else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equalsIgnoreCase(engineLvlRestriction)) limit = engineLvlMaxResponseTime;
			if (limit != null) {
				mergeIntoBucket(engineScopeBuckets, engineLvlRestriction, engineLvlFrequency, limit.doubleValue(), "engine", null);
			} else {
				classLogger.warn("Unknown engine level restriction type = '" + engineLvlRestriction + "' for user = " + User.getSingleLogginName(user));
			}
		}

		List<Map<String, Object>> groupPermissions = SecurityEngineUtils.getGroupEngineUsagePermissionMap(user, engineId);
		if (groupPermissions != null) {
			for (Map<String, Object> groupPerm : groupPermissions) {
				String groupRestriction = (String) groupPerm.get(Constants.GROUP_USAGE_RESTRICTION_KEY);
				String groupFrequency   = (String) groupPerm.get(Constants.GROUP_USAGE_FREQUENCY_KEY);
				String groupId          = (String) groupPerm.get("group_id");
				if (groupRestriction == null || groupRestriction.isEmpty()) continue;
				Number limit = null;
				if      (Constants.MODEL_CREDIT_RESTRICTION_VALUE.equalsIgnoreCase(groupRestriction))       limit = (Number) groupPerm.get(Constants.GROUP_MAX_CREDIT_KEY);
				else if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(groupRestriction))        limit = (Number) groupPerm.get(Constants.GROUP_MAX_TOKEN_KEY);
				else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equalsIgnoreCase(groupRestriction)) limit = (Number) groupPerm.get(Constants.GROUP_MAX_RESPONSE_TIME_KEY);
				if (limit != null) {
					mergeIntoBucket(engineScopeBuckets, groupRestriction, groupFrequency, limit.doubleValue(), "group", groupId);
				}
			}
		}

		// Enforce each engine-scope bucket independently; collect results for the list
		List<Map<String, Object>> allRestrictions = new ArrayList<>();

		for (Map<String, Object> bucket : engineScopeBuckets.values()) {
			String bType      = (String) bucket.get("type");
			String bFrequency = (String) bucket.get("frequency");
			double bLimit     = (Double)  bucket.get("limit");
			String bSource    = (String) bucket.get("limitSource");
			String bGroupId   = (String) bucket.get("limitSourceName");

			Number currentUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
					bType, user, engineId, currentDateTime, bFrequency);

			if (Constants.MODEL_CREDIT_RESTRICTION_VALUE.equalsIgnoreCase(bType)) {
				if (currentUsage.doubleValue() > bLimit)
					throw new IllegalArgumentException(String.format(ENGINE_CREDIT_LIMIT_EXCEEDED_MESSAGE, currentUsage.doubleValue(), bLimit));
			} else if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(bType)) {
				if (currentUsage.intValue() > (int) bLimit)
					throw new IllegalArgumentException(String.format(ENGINE_TOKEN_LIMIT_EXCEEDED_MESSAGE, currentUsage.intValue(), (int) bLimit));
			} else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equalsIgnoreCase(bType)) {
				if (currentUsage.doubleValue() > bLimit)
					throw new IllegalArgumentException(String.format(ENGINE_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE, currentUsage.doubleValue(), bLimit));
			}

			allRestrictions.add(buildRestrictionEntry(bType, bFrequency, bLimit, currentUsage, bSource, bGroupId));
		}

		// === USER-SCOPE RESTRICTION (cross-engine, always additive) ===
		if (userLvlRestriction != null && !userLvlRestriction.isEmpty()) {
			Number userLimit = null;
			if      (Constants.MODEL_CREDIT_RESTRICTION_VALUE.equalsIgnoreCase(userLvlRestriction))       userLimit = userLvlMaxCredits;
			else if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(userLvlRestriction))        userLimit = userLvlMaxTokens;
			else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equalsIgnoreCase(userLvlRestriction)) userLimit = userLvlMaxResponseTime;

			if (userLimit != null) {
				Number userCurrentUsage = ModelInferenceLogsUtils.getTotalUsageForUser(
						userLvlRestriction, user, engineId, currentDateTime, userLvlFrequency);

				if (Constants.MODEL_CREDIT_RESTRICTION_VALUE.equalsIgnoreCase(userLvlRestriction)) {
					if (userCurrentUsage.doubleValue() > userLimit.doubleValue())
						throw new IllegalArgumentException(String.format(USER_CREDIT_LIMIT_EXCEEDED_MESSAGE, userCurrentUsage.doubleValue(), userLimit.doubleValue()));
				} else if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(userLvlRestriction)) {
					if (userCurrentUsage.intValue() > userLimit.intValue())
						throw new IllegalArgumentException(String.format(USER_TOKEN_LIMIT_EXCEEDED_MESSAGE, userCurrentUsage.intValue(), userLimit.intValue()));
				} else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equalsIgnoreCase(userLvlRestriction)) {
					if (userCurrentUsage.doubleValue() > userLimit.doubleValue())
						throw new IllegalArgumentException(String.format(USER_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE, userCurrentUsage.doubleValue(), userLimit.doubleValue()));
				}

				allRestrictions.add(buildRestrictionEntry(userLvlRestriction, userLvlFrequency, userLimit.doubleValue(), userCurrentUsage, "user", null));
			} else {
				classLogger.warn("Unknown user level restriction type = '" + userLvlRestriction + "' for user = " + User.getSingleLogginName(user));
			}
		}

		// === POPULATE FLAT MAP WITH PRIMARY + FULL RESTRICTIONS LIST ===
		// Sort: credit first, then by frequency (DAY → WEEK → MONTH → YEAR → ALL_TIME).
		// First item after sort becomes the primary shown in the flat map.
		if (!allRestrictions.isEmpty()) {
			allRestrictions.sort(Comparator
					.comparingInt((Map<String, Object> e) -> typePriority((String) e.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE)))
					.thenComparingInt(e -> frequencyPriority((String) e.get(AbstractModelEngineResponse.USAGE_RESTRICTION_FREQUENCY)))
					.thenComparingDouble((Map<String, Object> e) -> -((Number) e.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE)).doubleValue()));

			Map<String, Object> primary = allRestrictions.get(0);
			String pType    = (String) primary.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE);
			String pSource  = (String) primary.get(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE);
			String pGroupId = (String) primary.get(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE_NAME);
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,        pType);
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_FREQUENCY,   primary.get(AbstractModelEngineResponse.USAGE_RESTRICTION_FREQUENCY));
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE, pSource);
			if (pGroupId != null) userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE_NAME, pGroupId);
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, primary.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE));
			userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,     primary.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE));
			userRestrictionMap.put(AbstractModelEngineResponse.RESTRICTIONS, allRestrictions);
		}

		return userRestrictionMap;
	}

	private static int typePriority(String restrictionType) {
		if (Constants.MODEL_CREDIT_RESTRICTION_VALUE.equalsIgnoreCase(restrictionType))       return 0;
		if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(restrictionType))        return 1;
		return 2;
	}

	private static int frequencyPriority(String frequency) {
		if (frequency == null) return 0;
		switch (frequency.toUpperCase()) {
			case "DAY":      return 0;
			case "WEEK":     return 1;
			case "MONTH":    return 2;
			case "YEAR":     return 3;
			case "ALL_TIME": return 4;
			default:         return 0;
		}
	}

	private static void mergeIntoBucket(Map<String, Map<String, Object>> buckets,
			String type, String frequency, double limit, String limitSource, String limitSourceName) {
		String key = type.toLowerCase() + "|" + (frequency == null ? "" : frequency.toUpperCase());
		Map<String, Object> existing = buckets.get(key);
		if (existing == null || limit > (Double) existing.get("limit")) {
			Map<String, Object> bucket = new HashMap<>();
			bucket.put("type",            type);
			bucket.put("frequency",       frequency);
			bucket.put("limit",           limit);
			bucket.put("limitSource",     limitSource);
			bucket.put("limitSourceName", limitSourceName);
			buckets.put(key, bucket);
		}
	}

	private static Map<String, Object> buildRestrictionEntry(String type, String frequency, double limit,
			Number currentUsage, String limitSource, String limitSourceName) {
		Map<String, Object> entry = new HashMap<>();
		entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,      type);
		entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_FREQUENCY,  frequency);
		entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE, limitSource);
		if (limitSourceName != null) entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE_NAME, limitSourceName);
		if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(type)) {
			entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, currentUsage.intValue());
			entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,     (int) limit);
		} else {
			entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, currentUsage.doubleValue());
			entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,     limit);
		}
		return entry;
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
		updateRestrictionMapCurrentUsage(userRestrictionMap, modelResponse, inputTime, outputTime, null, null, null,
				null);
	}

	public static void updateRestrictionMapCurrentUsage(Map<String, Object> userRestrictionMap,
			AbstractModelEngineResponse<?> modelResponse, ZonedDateTime inputTime, ZonedDateTime outputTime,
			Double inputTokenCredit, Double outputTokenCredit, Double cacheReadMultiplier,
			Double cacheWriteMultiplier) {
		if (userRestrictionMap != null && !userRestrictionMap.isEmpty()) {
			String restrictionMode = (String) userRestrictionMap
					.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE);

			if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(restrictionMode)) {
				userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
						((Number) userRestrictionMap.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE))
								.intValue() + modelResponse.getNumberOfTokensInPrompt()
								+ modelResponse.getNumberOfTokensInResponse());

			} else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equals(restrictionMode)) {

				Duration duration = Duration.between(inputTime, outputTime);
				long millisecondsDifference = duration.toMillis();
				Double millisecondsDouble = (double) millisecondsDifference;

				userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
						((Number) userRestrictionMap.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE))
								.doubleValue() + millisecondsDouble);

			} else if (Constants.MODEL_CREDIT_RESTRICTION_VALUE.equalsIgnoreCase(restrictionMode)
					&& inputTokenCredit != null && outputTokenCredit != null) {
				int inputTotal   = modelResponse.getNumberOfTokensInPrompt()    != null ? modelResponse.getNumberOfTokensInPrompt()    : 0;
				int cacheRead    = modelResponse.getNumberOfCacheReadTokens()   != null ? modelResponse.getNumberOfCacheReadTokens()   : 0;
				int cacheCreate  = modelResponse.getNumberOfCacheCreationTokens() != null ? modelResponse.getNumberOfCacheCreationTokens() : 0;
				int newTokens    = inputTotal - cacheRead - cacheCreate;
				int outputTokens = modelResponse.getNumberOfTokensInResponse()  != null ? modelResponse.getNumberOfTokensInResponse()  : 0;
				int thinking     = modelResponse.getNumberOfThinkingTokens()    != null ? modelResponse.getNumberOfThinkingTokens()    : 0;
				double readMult  = cacheReadMultiplier  != null ? cacheReadMultiplier  : 1.0;
				double writeMult = cacheWriteMultiplier != null ? cacheWriteMultiplier : 1.0;

				double requestBudget = newTokens    * inputTokenCredit
						+ cacheRead   * inputTokenCredit * readMult
						+ cacheCreate * inputTokenCredit * writeMult
						+ (outputTokens + thinking) * outputTokenCredit;

				userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
						((Number) userRestrictionMap.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE))
								.doubleValue() + requestBudget);
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
