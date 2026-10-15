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
package prerna.reactor.automation.run;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Searches authorized Automation run summaries without loading run payloads.
 *
 * <p>
 * Pixel:
 * {@code SearchAutomationRuns(project=["appId"], search=["text"], status=["FAILED"], triggerType=["SCHEDULED"], startDate=["2026-10-01T00:00:00Z"], endDate=["2026-10-15T00:00:00Z"], offset=["0"], limit=["50"])}.
 *
 * <p>
 * Selecting a returned run remains the responsibility of
 * {@link GetAutomationRunReactor}; this reactor never returns input snapshots or
 * node output payloads.
 */
public class SearchAutomationRunsReactor extends AbstractReactor {

	private static final String STATUS_KEY = "status";
	private static final String OFFSET_KEY = "offset";
	private static final int DEFAULT_LIMIT = 50;
	private static final int MAXIMUM_LIMIT = 100;
	private static final int MAXIMUM_SEARCH_LENGTH = 255;
	private static final Set<String> RUN_STATUSES = Set.of(AutomationConstants.STATUS_SUBMITTED,
			AutomationConstants.STATUS_RUNNING, AutomationConstants.STATUS_WAITING_FOR_INPUT,
			AutomationConstants.STATUS_SUCCESS, AutomationConstants.STATUS_FAILED,
			AutomationConstants.STATUS_INTERRUPTED, AutomationConstants.STATUS_CANCELLED);
	private static final Set<String> TRIGGER_TYPES = Set.of(AutomationConstants.TRIGGER_MANUAL,
			AutomationConstants.TRIGGER_PLAYGROUND, AutomationConstants.TRIGGER_SCHEDULED);

	public SearchAutomationRunsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.SEARCH.getKey(), STATUS_KEY,
				AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY, ReactorKeysEnum.START_DATE.getKey(),
				ReactorKeysEnum.END_DATE.getKey(), OFFSET_KEY, ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = AutomationProjectService.resolveRunScope(this.insight.getUser(),
				getString(ReactorKeysEnum.PROJECT.getKey()), false);
		String search = trimmedOptional(getString(ReactorKeysEnum.SEARCH.getKey()));
		String status = normalizedEnum(getString(STATUS_KEY));
		String triggerType = normalizedEnum(getString(AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY));
		long offset = getLong(OFFSET_KEY, 0L);
		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT);

		if (search != null && search.length() > MAXIMUM_SEARCH_LENGTH) {
			throw new IllegalArgumentException("Search text must not exceed " + MAXIMUM_SEARCH_LENGTH + " characters.");
		}
		if (status != null && !RUN_STATUSES.contains(status)) {
			throw new IllegalArgumentException("Status is not a supported Automation run status.");
		}
		if (triggerType != null && !TRIGGER_TYPES.contains(triggerType)) {
			throw new IllegalArgumentException("triggerType must be MANUAL, PLAYGROUND, or SCHEDULED.");
		}
		if (offset < 0) {
			throw new IllegalArgumentException("Offset must be non-negative.");
		}
		if (limit <= 0 || limit > MAXIMUM_LIMIT) {
			throw new IllegalArgumentException("Limit must be between 1 and " + MAXIMUM_LIMIT + ".");
		}

		Instant startedAfter = parseInstant(ReactorKeysEnum.START_DATE.getKey());
		Instant startedBefore = parseInstant(ReactorKeysEnum.END_DATE.getKey());
		if (startedAfter != null && startedBefore != null && startedAfter.isAfter(startedBefore)) {
			throw new IllegalArgumentException("startDate must be before or equal to endDate.");
		}

		List<Map<String, Object>> page = new ArrayList<>(AutomationRunStore.searchRunsForProject(projectId, search,
				status, triggerType, startedAfter, startedBefore, offset, limit + 1));
		boolean hasMore = page.size() > limit;
		if (hasMore) {
			page.remove(page.size() - 1);
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("runs", page);
		result.put(OFFSET_KEY, offset);
		result.put("nextOffset", offset + page.size());
		result.put("hasMore", hasMore);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private Instant parseInstant(String key) {
		String value = trimmedOptional(getString(key));
		if (value == null) {
			return null;
		}
		try {
			return Instant.parse(value);
		} catch (DateTimeParseException e) {
			throw new IllegalArgumentException(key + " must be an ISO-8601 timestamp.", e);
		}
	}

	private static String normalizedEnum(String value) {
		String normalized = trimmedOptional(value);
		return normalized == null ? null : normalized.toUpperCase(Locale.ROOT);
	}

	private static String trimmedOptional(String value) {
		return value == null || value.isBlank() ? null : value.trim();
	}

	@Override
	public String getReactorDescription() {
		return "Searches authorized Automation run summaries using bounded run-level filters.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.SEARCH.getKey().equals(key)) {
			return "Optional text matched against run ID, result summary, creator, and failure details.";
		}
		if (STATUS_KEY.equals(key)) {
			return "Optional exact Automation run status.";
		}
		if (AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY.equals(key)) {
			return "Optional exact trigger type: MANUAL, PLAYGROUND, or SCHEDULED.";
		}
		if (ReactorKeysEnum.START_DATE.getKey().equals(key) || ReactorKeysEnum.END_DATE.getKey().equals(key)) {
			return "Optional inclusive ISO-8601 UTC run-start boundary.";
		}
		if (OFFSET_KEY.equals(key)) {
			return "Optional non-negative result offset.";
		}
		if (ReactorKeysEnum.LIMIT.getKey().equals(key)) {
			return "Optional page size from 1 through " + MAXIMUM_LIMIT + ".";
		}
		return super.getDescriptionForKey(key);
	}
}
