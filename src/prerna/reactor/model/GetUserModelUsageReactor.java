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

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetUserModelUsageReactor extends AbstractReactor {

	public GetUserModelUsageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.START_DATE.getKey(),
				ReactorKeysEnum.END_DATE.getKey() };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = insight.getUser();

		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		// Get the parameters
		List<String> engineIds = getList(ReactorKeysEnum.ENGINE.getKey());
		// Validate we have at least one engine
		if (engineIds == null || engineIds.isEmpty()) {
			throw new IllegalArgumentException("At least one engine ID must be provided");
		}

		String startDate = this.keyValue.get(ReactorKeysEnum.START_DATE.getKey());
		String endDate = this.keyValue.get(ReactorKeysEnum.END_DATE.getKey());
		// Validate date parameters
		validateDateParameters(startDate, endDate);

		// Get usage data per engine
		List<Map<String, Object>> usageData = ModelInferenceLogsUtils.getUserModelUsagePerEngine(user, engineIds,
				startDate, endDate);

		// Look up engine names from the input engineIds — always use the input list so
		// engines with zero usage still get a named zero row
		Map<Object, Object> idToAlias = SecurityEngineUtils.getEngineAliasForIds(engineIds);

		// Index returned usage rows by engine ID for easy lookup
		List<String> detailKeys = Arrays.asList(
				"DETAIL_INPUT_TOKENS", "DETAIL_OUTPUT_TOKENS",
				"DETAIL_CACHE_READ_TOKENS", "DETAIL_CACHE_CREATION_TOKENS", "DETAIL_THINKING_TOKENS");

		Map<String, Map<String, Object>> usageByEngineId = new HashMap<>();
		for (Map<String, Object> entry : usageData) {
			String engineId = (String) entry.get("ENGINE_ID");
			if (engineId != null) {
				// Nest DETAIL_* columns into TOKEN_DETAIL, stripping the prefix
				Map<String, Object> tokenDetail = new HashMap<>();
				for (String detailKey : detailKeys) {
					tokenDetail.put(detailKey.substring("DETAIL_".length()), entry.remove(detailKey));
				}
				entry.put("TOKEN_DETAIL", tokenDetail);
				entry.put("ENGINE_NAME", idToAlias.get(engineId));
				usageByEngineId.put(engineId, entry);
			}
		}

		// Ensure every requested engine has a row — fill zeros for engines with no usage
		List<Map<String, Object>> result = new ArrayList<>();
		for (String engineId : engineIds) {
			if (usageByEngineId.containsKey(engineId)) {
				result.add(usageByEngineId.get(engineId));
			} else {
				Map<String, Object> zeroRow = new HashMap<>();
				zeroRow.put("ENGINE_ID", engineId);
				zeroRow.put("ENGINE_NAME", idToAlias.get(engineId));
				zeroRow.put("INPUT_TOKENS", 0);
				zeroRow.put("RESPONSE_TOKENS", 0);
				zeroRow.put("TOTAL_TOKENS", 0);
				zeroRow.put("TOTAL_REQUESTS", 0);
				Map<String, Object> tokenDetail = new HashMap<>();
				tokenDetail.put("INPUT_TOKENS", 0);
				tokenDetail.put("OUTPUT_TOKENS", 0);
				tokenDetail.put("CACHE_READ_TOKENS", 0);
				tokenDetail.put("CACHE_CREATION_TOKENS", 0);
				tokenDetail.put("THINKING_TOKENS", 0);
				zeroRow.put("TOKEN_DETAIL", tokenDetail);
				result.add(zeroRow);
			}
		}

		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	/**
	 * Validates that if one date is provided, both must be provided, that dates are
	 * valid, and that start date is before or equal to end date
	 * 
	 * @param startDate
	 * @param endDate
	 */
	private void validateDateParameters(String startDate, String endDate) {
		boolean hasStartDate = startDate != null && !startDate.trim().isEmpty();
		boolean hasEndDate = endDate != null && !endDate.trim().isEmpty();

		if (hasStartDate != hasEndDate) {
			throw new IllegalArgumentException(
					"Both startDate and endDate must be provided together, or neither should be provided");
		}

		// If both dates are provided, validate them
		if (hasStartDate && hasEndDate) {
			DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd")
					.parseDefaulting(ChronoField.HOUR_OF_DAY, 0).parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
					.parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0).toFormatter().withZone(ZoneOffset.UTC);
			ZonedDateTime start;
			ZonedDateTime end;

			// Parse and validate start date
			try {
				start = ZonedDateTime.parse(startDate.trim(), formatter);
			} catch (DateTimeParseException e) {
				throw new IllegalArgumentException(
						"Invalid startDate format. Expected format: YYYY-MM-DD (e.g., 2026-01-15)");
			}

			// Parse and validate end date
			try {
				end = ZonedDateTime.parse(endDate.trim(), formatter);
			} catch (DateTimeParseException e) {
				throw new IllegalArgumentException(
						"Invalid endDate format. Expected format: YYYY-MM-DD (e.g., 2026-01-15)");
			}

			// Validate start date is not in the future
			ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
			if (start.isAfter(now)) {
				throw new IllegalArgumentException("startDate cannot be in the future. Provided: startDate=" + startDate);
			}

			// Validate start date is before or equal to end date
			if (start.isAfter(end)) {
				throw new IllegalArgumentException("startDate must be before or equal to endDate. Provided: startDate="
						+ startDate + ", endDate=" + endDate);
			}
		}
	}

	@Override
	public String getReactorDescription() {
		return """
				Returns model usage for the current user over a specified time period, broken down by engine. \
				Reports INPUT_TOKENS, OUTPUT_TOKENS, CACHE_READ_TOKENS, CACHE_CREATION_TOKENS, THINKING_TOKENS, \
				TOTAL_TOKENS, and TOTAL_REQUESTS. Requires a list of engine IDs and optionally accepts a date range.
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "Required list of engine IDs to get usage for (can be a single engine or multiple engines).";
		} else if (key.equals(ReactorKeysEnum.START_DATE.getKey())) {
			return "Optional start date (format: YYYY-MM-DD). Must be provided with endDate.";
		} else if (key.equals(ReactorKeysEnum.END_DATE.getKey())) {
			return "Optional end date (format: YYYY-MM-DD). Must be provided with startDate.";
		}
		return super.getDescriptionForKey(key);
	}
}
