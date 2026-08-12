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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityModelMetadataUtils;
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

	// Applied when a model has never had a cache weight configured, so counting
	// cache tokens toward a "token" restriction starts out equivalent to counting
	// them as regular tokens rather than silently ignoring them.
	private static final double DEFAULT_CACHE_TOKEN_WEIGHT_PERCENT = 100.0;

	/**
	 * Look up the admin-configured cache token weights for a model engine, in
	 * [cacheReadWeightPercent, cacheWriteWeightPercent] order. Falls back to
	 * {@link #DEFAULT_CACHE_TOKEN_WEIGHT_PERCENT} for either value that has never
	 * been set.
	 *
	 * @param engineId the model engine
	 * @return the two weight percentages
	 */
	private static double[] resolveCacheTokenWeightPercents(String engineId) {
		Map<String, Object> metadata = SecurityModelMetadataUtils.getModelMetadata(engineId);
		Object cacheReadWeight = metadata == null ? null : metadata.get("cacheReadWeight");
		Object cacheWriteWeight = metadata == null ? null : metadata.get("cacheWriteWeight");
		return new double[] {
				cacheReadWeight instanceof Number number ? number.doubleValue() : DEFAULT_CACHE_TOKEN_WEIGHT_PERCENT,
				cacheWriteWeight instanceof Number number ? number.doubleValue() : DEFAULT_CACHE_TOKEN_WEIGHT_PERCENT };
	}

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
							engineLvlModelUsageFrequency, 0.0, 0.0);

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

				} else if (Constants.MODEL_TOKEN_CACHE_RESTRICTION_VALUE
						.equalsIgnoreCase(engineLvlModelUsageRestriction)) {
					double[] cacheWeights = resolveCacheTokenWeightPercents(engineId);
					currentUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
							Constants.MODEL_TOKEN_CACHE_RESTRICTION_VALUE, user, engineId, currentDateTime,
							engineLvlModelUsageFrequency, cacheWeights[0], cacheWeights[1]);

					if (currentUsage.intValue() > engineLvlModelUsageMaxTokens.intValue()) {
						throw new IllegalArgumentException(String.format(ENGINE_TOKEN_LIMIT_EXCEEDED_MESSAGE,
								currentUsage.intValue(), engineLvlModelUsageMaxTokens.intValue()));
					}

					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,
							Constants.MODEL_TOKEN_CACHE_RESTRICTION_VALUE);
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
							currentUsage.intValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,
							engineLvlModelUsageMaxTokens.intValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CACHE_READ_WEIGHT,
							cacheWeights[0]);
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CACHE_WRITE_WEIGHT,
							cacheWeights[1]);

				} else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE
						.equalsIgnoreCase(engineLvlModelUsageRestriction)) {
					currentUsage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
							Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE, user, engineId, currentDateTime,
							engineLvlModelUsageFrequency, DEFAULT_CACHE_TOKEN_WEIGHT_PERCENT,
							DEFAULT_CACHE_TOKEN_WEIGHT_PERCENT);

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
							user, engineId, currentDateTime, userLvlModelUsageFrequency, 0.0, 0.0);

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

				} else if (Constants.MODEL_TOKEN_CACHE_RESTRICTION_VALUE
						.equalsIgnoreCase(userLvlModelUsageRestriction)) {

					double[] cacheWeights = resolveCacheTokenWeightPercents(engineId);
					currentUsage = ModelInferenceLogsUtils.getTotalUsageForUser(
							Constants.MODEL_TOKEN_CACHE_RESTRICTION_VALUE, user, engineId, currentDateTime,
							userLvlModelUsageFrequency, cacheWeights[0], cacheWeights[1]);

					if (currentUsage.intValue() > userLvlModelUsageMaxTokens.intValue()) {
						throw new IllegalArgumentException(String.format(USER_TOKEN_LIMIT_EXCEEDED_MESSAGE,
								currentUsage.intValue(), userLvlModelUsageMaxTokens.intValue()));
					}
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE,
							Constants.MODEL_TOKEN_CACHE_RESTRICTION_VALUE);
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
							currentUsage.intValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_MAX_VALUE,
							userLvlModelUsageMaxTokens.intValue());
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CACHE_READ_WEIGHT,
							cacheWeights[0]);
					userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CACHE_WRITE_WEIGHT,
							cacheWeights[1]);

				} else if (Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE
						.equalsIgnoreCase(userLvlModelUsageRestriction)) {

					currentUsage = ModelInferenceLogsUtils.getTotalUsageForUser(
							Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE, user, engineId, currentDateTime,
							userLvlModelUsageFrequency, DEFAULT_CACHE_TOKEN_WEIGHT_PERCENT,
							DEFAULT_CACHE_TOKEN_WEIGHT_PERCENT);

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
		if (userRestrictionMap != null && !userRestrictionMap.isEmpty()) {
			String restrictionMode = (String) userRestrictionMap
					.get(AbstractModelEngineResponse.USAGE_RESTRICTION_MODE);

			if (Constants.MODEL_TOKEN_RESTRICTION_VALUE.equalsIgnoreCase(restrictionMode)
					|| Constants.MODEL_TOKEN_CACHE_RESTRICTION_VALUE.equalsIgnoreCase(restrictionMode)) {
				// plain "token" mode never stashes a weight, so 0 (i.e. cache tokens do not
				// count) is the correct default there; "token_cache" mode always stashes the
				// model's resolved weight (itself defaulting to 100 when unconfigured)
				double cacheReadWeightPercent = ((Number) userRestrictionMap
						.getOrDefault(AbstractModelEngineResponse.USAGE_RESTRICTION_CACHE_READ_WEIGHT, 0.0))
						.doubleValue();
				double cacheWriteWeightPercent = ((Number) userRestrictionMap
						.getOrDefault(AbstractModelEngineResponse.USAGE_RESTRICTION_CACHE_WRITE_WEIGHT, 0.0))
						.doubleValue();
				int cacheReadTokens = modelResponse.getNumberOfCacheReadTokens() == null ? 0
						: modelResponse.getNumberOfCacheReadTokens();
				int cacheCreationTokens = modelResponse.getNumberOfCacheCreationTokens() == null ? 0
						: modelResponse.getNumberOfCacheCreationTokens();
				double weightedCacheTokens = cacheReadTokens * (cacheReadWeightPercent / 100.0)
						+ cacheCreationTokens * (cacheWriteWeightPercent / 100.0);

				userRestrictionMap.put(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE,
						// put in the new value of the current usage we calculated + the number of
						// tokens we just created, plus any cache tokens at their configured weight
						((Number) userRestrictionMap.get(AbstractModelEngineResponse.USAGE_RESTRICTION_CURRENT_VALUE))
								.intValue() + modelResponse.getNumberOfTokensInPrompt()
								+ modelResponse.getNumberOfTokensInResponse() + (int) Math.round(weightedCacheTokens));

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
