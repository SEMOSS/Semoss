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
package prerna.reactor.model;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.ModelUsageRestrictionUtility;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Returns the authenticated user's credit configuration and current usage for
 * one model engine.
 */
public class GetUserModelCreditInfoReactor extends AbstractReactor {

	private static final double TOKENS_PER_MILLION = 1_000_000D;
	private static final String USER_ID_KEY = "userId";

	public GetUserModelCreditInfoReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), USER_ID_KEY };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must input a model engine id");
		}

		String callerUserId = user.getPrimaryLoginToken().getId();
		String requestedUserId = this.keyValue.get(USER_ID_KEY);
		String targetUserId = requestedUserId == null || requestedUserId.trim().isEmpty()
				? callerUserId
				: requestedUserId.trim();
		boolean viewingAnotherUser = !callerUserId.equals(targetUserId);
		if (viewingAnotherUser) {
			if (SecurityAdminUtils.getInstance(user) == null) {
				throw new IllegalArgumentException("User must be an admin to view another user's model credit usage");
			}
			if (!SecurityQueryUtils.checkUserExist(targetUserId)) {
				throw new IllegalArgumentException("User " + targetUserId + " does not exist");
			}
		}

		engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId.trim());
		if (!viewingAnotherUser && !SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}
		if (SecurityEngineUtils.getEngineType(engineId) != IEngine.CATALOG_TYPE.MODEL) {
			throw new IllegalArgumentException("Engine is not a model engine");
		}

		Map<String, Object> metadata = SecurityModelMetadataUtils.getModelMetadata(engineId);
		if (metadata == null) {
			metadata = Collections.emptyMap();
		}

		Double inputTokenCredit = asDouble(metadata.get("inputTokenCredit"));
		Double outputTokenCredit = asDouble(metadata.get("outputTokenCredit"));
		Double cacheReadMultiplier = asDouble(metadata.get("cacheReadMultiplier"));
		Double cacheWriteMultiplier = asDouble(metadata.get("cacheWriteMultiplier"));

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("engineId", engineId);
		result.put("userId", targetUserId);
		result.put("inputTokenCredit", inputTokenCredit);
		result.put("outputTokenCredit", outputTokenCredit);
		result.put("inputCreditsPerMillion", multiply(inputTokenCredit, TOKENS_PER_MILLION));
		result.put("outputCreditsPerMillion", multiply(outputTokenCredit, TOKENS_PER_MILLION));
		result.put("cacheReadMultiplier", cacheReadMultiplier == null ? 1D : cacheReadMultiplier);
		result.put("cacheWriteMultiplier", cacheWriteMultiplier == null ? 1D : cacheWriteMultiplier);
		result.put("pricingConfigured", inputTokenCredit != null && outputTokenCredit != null);

		Map<String, Object> permission = getPermission(targetUserId, engineId);
		String restrictionType = asString(permission.get(Constants.ENGINE_USAGE_RESTRICTION_KEY));
		String configuredFrequency = asString(permission.get(Constants.ENGINE_USAGE_FREQUENCY_KEY));
		Number maxCredits = asNumber(permission.get(Constants.ENGINE_MAX_CREDIT_KEY));
		boolean restrictionEnabled = Constants.MODEL_CREDIT_RESTRICTION_VALUE.equalsIgnoreCase(restrictionType)
				&& maxCredits != null;
		boolean trackingEnabled = Utility.isModelInferenceLogsEnabled();

		result.put("restrictionEnabled", restrictionEnabled);
		result.put("restrictionType", restrictionType);
		result.put("frequency", restrictionEnabled ? normalizeFrequency(configuredFrequency) : configuredFrequency);
		result.put("maxCredits", maxCredits == null ? null : maxCredits.doubleValue());
		result.put("trackingEnabled", trackingEnabled);

		if (restrictionEnabled && trackingEnabled) {
			String frequency = normalizeFrequency(configuredFrequency);
			ZonedDateTime now = Utility.getCurrentZonedDateTimeUTC();
			Number usage = ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime(
					Constants.MODEL_CREDIT_RESTRICTION_VALUE, targetUserId, engineId, now, frequency);
			double creditsUsed = usage == null ? 0D : usage.doubleValue();
			double maximum = maxCredits.doubleValue();
			Map<String, ZonedDateTime> period = ModelUsageRestrictionUtility.getDateRangeFromFrequency(frequency, now);

			result.put("creditsUsed", creditsUsed);
			result.put("creditsRemaining", Math.max(0D, maximum - creditsUsed));
			result.put("limitExceeded", creditsUsed > maximum);
			result.put("periodStart", period.get("start").toString());
			result.put("periodEnd", period.get("end").toString());
		} else {
			result.put("creditsUsed", null);
			result.put("creditsRemaining", null);
			result.put("limitExceeded", null);
			result.put("periodStart", null);
			result.put("periodEnd", null);
		}

		return new NounMetadata(result, PixelDataType.MAP);
	}

	private static Map<String, Object> getPermission(String userId, String engineId) {
		List<Map<String, Object>> permissions = SecurityEngineUtils.getEngineUsagePermissionMapForUserId(userId, engineId);
		if (permissions == null || permissions.isEmpty() || permissions.get(0) == null) {
			return Collections.emptyMap();
		}
		return permissions.get(0);
	}

	private static String normalizeFrequency(String frequency) {
		if (frequency == null) {
			return "DAY";
		}
		String normalized = frequency.trim().toUpperCase();
		if (normalized.equals("WEEK") || normalized.equals("MONTH") || normalized.equals("YEAR")
				|| normalized.equals("ALL_TIME")) {
			return normalized;
		}
		return "DAY";
	}

	private static String asString(Object value) {
		return value == null ? null : value.toString();
	}

	private static Number asNumber(Object value) {
		return value instanceof Number ? (Number) value : null;
	}

	private static Double asDouble(Object value) {
		return value instanceof Number ? ((Number) value).doubleValue() : null;
	}

	private static Double multiply(Double value, double multiplier) {
		return value == null ? null : value * multiplier;
	}

	@Override
	public String getReactorDescription() {
		return "Returns the authenticated user's credit limit, usage, remaining balance, and pricing for a model";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "Model engine id or alias for which to retrieve user credit information";
		}
		if (key.equals(USER_ID_KEY)) {
			return "Optional user id; defaults to the current user and requires admin access for another user";
		}
		return super.getDescriptionForKey(key);
	}
}
