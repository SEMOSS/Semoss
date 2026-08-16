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
		Map<String, Object> userRestrictionMap = new HashMap<>();

		List<Map<String, Object>> engineUserPermission = SecurityEngineUtils.getEngineUsagePermissionMap(user,
				engineId);
		if (engineUserPermission != null && !engineUserPermission.isEmpty()) {
			// there should only 1 row in this object
			Map<String, Object> engineUserPermissionMap = engineUserPermission.get(0);
			// lets see if any restriction is applied

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

			ZonedDateTime currentDateTime = Utility.getCurrentZonedDateTimeUTC();

			Number currentUsage = null;
			// engine specific restriction
			if (engineLvlModelUsageRestriction != null && !engineLvlModelUsageRestriction.isEmpty()) {
				if (!Utility.isModelInferenceLogsEnabled()) {
					throw new IllegalArgumentException(
							"Model restrictions have been enabled but not properly configured on the platform. Please reach out to a system administrator");
				}

				if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(engineLvlModelUsageRestriction)) {
					currentUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
							Constants.MODEL_TOKEN_RESTRICTION_VALUE, user, engineId, currentDateTime,
							engineLvlModelUsageFrequency);

					if (currentUsage.intValue() > engineLvlModelUsageMaxTokens.intValue()) {
						throw new IllegalArgumentException(String.format(ENGINE_TOKEN_LIMIT_EXCEEDED_MESSAGE,
								currentUsage.intValue(), engineLvlModelUsageMaxTokens.intValue()));
					}

					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,
							Constants.MODEL_TOKEN_RESTRICTION_VALUE);
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
							currentUsage.intValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,
							engineLvlModelUsageMaxTokens.intValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE, "engine");
					List<Map<String, Object>> rl = new ArrayList<>();
					rl.add(new HashMap<>(userRestrictionMap));
					userRestrictionMap.put(AbstractModelEngineResponse.RESTRICTIONS, rl);

				} else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE
						.equalsIgnoreCase(engineLvlModelUsageRestriction)) {
					currentUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
							Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE, user, engineId, currentDateTime,
							engineLvlModelUsageFrequency);

					if (currentUsage.doubleValue() > engineLvlModelUsageMaxResponseTime.doubleValue()) {
						throw new IllegalArgumentException(String.format(ENGINE_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE,
								currentUsage.doubleValue(), engineLvlModelUsageMaxResponseTime.doubleValue()));
					}

					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,
							Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE);
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
							currentUsage.intValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,
							engineLvlModelUsageMaxResponseTime.intValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE, "engine");
					List<Map<String, Object>> rl = new ArrayList<>();
					rl.add(new HashMap<>(userRestrictionMap));
					userRestrictionMap.put(AbstractModelEngineResponse.RESTRICTIONS, rl);

				} else if (Constants.MODEL_CREDIT_RESTRICTION_VALUE
						.equalsIgnoreCase(engineLvlModelUsageRestriction)) {
					Number engineLvlModelUsageMaxCredits = (Number) engineUserPermissionMap
							.get(Constants.ENGINE_MAX_CREDIT_KEY);
					currentUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
							Constants.MODEL_CREDIT_RESTRICTION_VALUE, user, engineId, currentDateTime,
							engineLvlModelUsageFrequency);

					if (currentUsage.doubleValue() > engineLvlModelUsageMaxCredits.doubleValue()) {
						throw new IllegalArgumentException(String.format(ENGINE_CREDIT_LIMIT_EXCEEDED_MESSAGE,
								currentUsage.doubleValue(), engineLvlModelUsageMaxCredits.doubleValue()));
					}

					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,
							Constants.MODEL_CREDIT_RESTRICTION_VALUE);
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
							currentUsage.doubleValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,
							engineLvlModelUsageMaxCredits.doubleValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE, "engine");
					List<Map<String, Object>> rl = new ArrayList<>();
					rl.add(new HashMap<>(userRestrictionMap));
					userRestrictionMap.put(AbstractModelEngineResponse.RESTRICTIONS, rl);

				} else {
					classLogger.warn("Unknown engine level model restriction type = '" + engineLvlModelUsageRestriction
							+ "' for user = " + User.getSingleLogginName(user));
				}
			}
			// user general restriction
			else if (userLvlModelUsageRestriction != null && !userLvlModelUsageRestriction.isEmpty()) {
				if (!Utility.isModelInferenceLogsEnabled()) {
					throw new IllegalArgumentException(
							"User model restrictions have been enabled but not properly configured on the platform. Please reach out to a system administrator");
				}

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
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE, "user");
					List<Map<String, Object>> rl = new ArrayList<>();
					rl.add(new HashMap<>(userRestrictionMap));
					userRestrictionMap.put(AbstractModelEngineResponse.RESTRICTIONS, rl);

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
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE, "user");
					List<Map<String, Object>> rl = new ArrayList<>();
					rl.add(new HashMap<>(userRestrictionMap));
					userRestrictionMap.put(AbstractModelEngineResponse.RESTRICTIONS, rl);

				} else if (Constants.MODEL_CREDIT_RESTRICTION_VALUE.equalsIgnoreCase(userLvlModelUsageRestriction)) {
					Number userLvlModelUsageMaxCredits = (Number) engineUserPermissionMap
							.get(Constants.USER_MODEL_MAX_CREDIT_KEY);

					currentUsage = ModelInferenceLogsUtils.getTotalUsageForUser(
							Constants.MODEL_CREDIT_RESTRICTION_VALUE, user, engineId, currentDateTime,
							userLvlModelUsageFrequency);

					if (currentUsage.doubleValue() > userLvlModelUsageMaxCredits.doubleValue()) {
						throw new IllegalArgumentException(String.format(USER_CREDIT_LIMIT_EXCEEDED_MESSAGE,
								currentUsage.doubleValue(), userLvlModelUsageMaxCredits.doubleValue()));
					}
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,
							Constants.MODEL_CREDIT_RESTRICTION_VALUE);
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
							currentUsage.doubleValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,
							userLvlModelUsageMaxCredits.doubleValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE, "user");
					List<Map<String, Object>> rl = new ArrayList<>();
					rl.add(new HashMap<>(userRestrictionMap));
					userRestrictionMap.put(AbstractModelEngineResponse.RESTRICTIONS, rl);

				} else {
					classLogger.warn("Unknown user level model restriction type = '" + userLvlModelUsageRestriction
							+ "' for user = " + User.getSingleLogginName(user));
				}
			}
			// group fallback — only reached when no individual engine-level or user-level
			// restriction applies. Each distinct (type, frequency) pair is an independent
			// constraint that must pass. Within the same (type, frequency) bucket, groups
			// are unioned — the user gets the MAX (most permissive) limit across all groups
			// that share that bucket. Different types or different frequencies must each pass.
			else if (Utility.isModelInferenceLogsEnabled()) {
				List<Map<String, Object>> groupPermissions = SecurityEngineUtils.getGroupEngineUsagePermissionMap(user, engineId);
				if (groupPermissions != null && !groupPermissions.isEmpty()) {
					// Build buckets keyed by "type|frequency". Within each bucket keep the MAX limit.
					Map<String, Map<String, Object>> limitBuckets = new HashMap<>();
					for (Map<String, Object> groupPerm : groupPermissions) {
						String groupRestriction = (String) groupPerm.get(Constants.GROUP_USAGE_RESTRICTION_KEY);
						String groupFrequency   = (String) groupPerm.get(Constants.GROUP_USAGE_FREQUENCY_KEY);
						String groupId          = (String) groupPerm.get("group_id");
						if (groupRestriction == null || groupRestriction.isEmpty()) continue;

						Number limit = null;
						if      (Constants.MODEL_CREDIT_RESTRICTION_VALUE.equalsIgnoreCase(groupRestriction))
							limit = (Number) groupPerm.get(Constants.GROUP_MAX_CREDIT_KEY);
						else if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(groupRestriction))
							limit = (Number) groupPerm.get(Constants.GROUP_MAX_TOKEN_KEY);
						else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equalsIgnoreCase(groupRestriction))
							limit = (Number) groupPerm.get(Constants.GROUP_MAX_RESPONSE_TIME_KEY);
						if (limit == null) continue;

						String bucketKey = groupRestriction.toLowerCase() + "|"
								+ (groupFrequency == null ? "" : groupFrequency.toUpperCase());
						Map<String, Object> existing = limitBuckets.get(bucketKey);
						if (existing == null || limit.doubleValue() > (Double) existing.get("limit")) {
							Map<String, Object> bucket = new HashMap<>();
							bucket.put("type",      groupRestriction);
							bucket.put("frequency", groupFrequency);
							bucket.put("limit",     limit.doubleValue());
							bucket.put("groupId",   groupId);
							limitBuckets.put(bucketKey, bucket);
						}
					}

					// Check every bucket independently; collect all passing results.
					// Primary = tightest bucket: credit(0) > token(1) > compute(2);
					// within the same type, smallest maxValue wins.
					List<Map<String, Object>> allGroupRestrictions = new ArrayList<>();
					Map<String, Object> primaryBucket = null;
					Number primaryUsage               = null;
					int    primaryTypePriority        = Integer.MAX_VALUE;
					double primaryLimit               = Double.MAX_VALUE;

					for (Map<String, Object> bucket : limitBuckets.values()) {
						String bType      = (String) bucket.get("type");
						String bFrequency = (String) bucket.get("frequency");
						double bLimit     = (Double)  bucket.get("limit");
						String bGroupId   = (String) bucket.get("groupId");

						currentUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
								bType, user, engineId, currentDateTime, bFrequency);

						if (Constants.MODEL_CREDIT_RESTRICTION_VALUE.equalsIgnoreCase(bType)) {
							if (currentUsage.doubleValue() > bLimit)
								throw new IllegalArgumentException(String.format(ENGINE_CREDIT_LIMIT_EXCEEDED_MESSAGE,
										currentUsage.doubleValue(), bLimit));
						} else if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(bType)) {
							if (currentUsage.intValue() > (int) bLimit)
								throw new IllegalArgumentException(String.format(ENGINE_TOKEN_LIMIT_EXCEEDED_MESSAGE,
										currentUsage.intValue(), (int) bLimit));
						} else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE.equalsIgnoreCase(bType)) {
							if (currentUsage.doubleValue() > bLimit)
								throw new IllegalArgumentException(String.format(ENGINE_RESPONSE_TIME_LIMIT_EXCEEDED_MESSAGE,
										currentUsage.doubleValue(), bLimit));
						}

						// Build list entry for this bucket
						Map<String, Object> entry = new HashMap<>();
						entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,             bType);
						entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE,      "group");
						entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE_NAME, bGroupId);
						entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_FREQUENCY,         bFrequency);
						if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(bType)) {
							entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, currentUsage.intValue());
							entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,     (int) bLimit);
						} else {
							entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, currentUsage.doubleValue());
							entry.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,     bLimit);
						}
						allGroupRestrictions.add(entry);

						// Track primary: credit(0) > token(1) > compute(2); within same type, smallest limit
						int bPriority = Constants.MODEL_CREDIT_RESTRICTION_VALUE.equalsIgnoreCase(bType) ? 0
								      : Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(bType)   ? 1 : 2;
						if (bPriority < primaryTypePriority
								|| (bPriority == primaryTypePriority && bLimit < primaryLimit)) {
							primaryBucket       = bucket;
							primaryUsage        = currentUsage;
							primaryTypePriority = bPriority;
							primaryLimit        = bLimit;
						}
					}

					if (primaryBucket != null) {
						String pType    = (String) primaryBucket.get("type");
						String pGroupId = (String) primaryBucket.get("groupId");
						userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,             pType);
						userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE,      "group");
						userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_LIMIT_SOURCE_NAME, pGroupId);
						if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(pType)) {
							userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, primaryUsage.intValue());
							userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,     (int) primaryLimit);
						} else {
							userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE, primaryUsage.doubleValue());
							userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,     primaryLimit);
						}
						userRestrictionMap.put(AbstractModelEngineResponse.RESTRICTIONS, allGroupRestrictions);
					}
				}
			}
		}

		return userRestrictionMap;
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
