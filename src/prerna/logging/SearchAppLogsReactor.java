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
package prerna.logging;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.io.input.ReversedLinesFileReader;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Searches a project's app log file (and its rotated siblings) on disk for
 * lines matching a text query and/or level filter. No database involved -
 * {@code app.log}'s configurable rotation already bounds how much there is to
 * search.
 * <p>
 * Pixel usage:
 * <pre>
 * SearchAppLogs(paramValues=[{
 *   "projectId": "X",
 *   "query": "timed out",       // optional, case-insensitive substring match
 *   "levels": "ERROR,WARN",     // optional, comma-separated
 *   "offset": "0",
 *   "limit": "50"
 * }]);
 * </pre>
 * Returns {@code {"lines": [...], "hasMore": bool}} - lines are newest-first.
 * <p>
 * Security: owner-only, same rule {@code InsightWebsocket}'s app_logs watch
 * gate uses - logs can expose request/response payloads and other users'
 * activity.
 */
public class SearchAppLogsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SearchAppLogsReactor.class);

	private static final int DEFAULT_LIMIT = 50;
	private static final int MAX_LIMIT = 500;
	private static final int MAX_OFFSET = 10_000;

	public SearchAppLogsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		if (!AppLogManager.isEnabled()) {
			throw new IllegalStateException("Application logging is disabled");
		}

		User user = this.insight.getUser();
		if (user == null || user.getPrimaryLoginToken() == null) {
			throwAnonymousUserError();
		}
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		Map<String, Object> params = getParamMap();
		String projectId = getString(params, SemossLogUtils.PROJECT_ID);
		if (projectId == null || projectId.isBlank()) {
			throw new IllegalArgumentException("Must provide 'projectId'");
		}

		String userId = user.getPrimaryLoginToken().getId();
		Integer permissionLvl = SecurityProjectUtils.getUserProjectPermission(userId, projectId);
		if (permissionLvl == null || !AccessPermissionEnum.isOwner(permissionLvl)) {
			throw new IllegalArgumentException("Only project owners can search app logs");
		}

		String query = getString(params, "query").toLowerCase();
		Set<String> levels = parseLevels(getString(params, "levels"));
		int offset = clampOffset(parseLong(getString(params, "offset"), 0L));
		int limit = clampLimit(parseLong(getString(params, "limit"), DEFAULT_LIMIT));

		String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
		List<File> files = resolveLogFiles(projectId, projectName);
		SearchResult searchResult = searchFiles(files, query, levels, offset, limit);
		return buildResult(searchResult.lines(), searchResult.hasMore());
	}

	static SearchResult searchFiles(List<File> files, String query, Set<String> levels, int offset, int limit) {
		List<String> matches = new ArrayList<>(limit);
		int matchedLines = 0;
		boolean hasMore = false;
		for (File file : files) {
			try (ReversedLinesFileReader reader = ReversedLinesFileReader.builder()
					.setFile(file)
					.setCharset(StandardCharsets.UTF_8)
					.get()) {
				String line;
				while ((line = reader.readLine()) != null) {
					if (!matchesFilter(line, query, levels)) {
						continue;
					}
					if (matchedLines >= offset) {
						if (matches.size() == limit) {
							hasMore = true;
							break;
						}
						matches.add(line);
					}
					matchedLines++;
				}
			} catch (IOException e) {
				classLogger.warn("Could not read log file '{}'", file.getPath(), e);
			}
			if (hasMore) {
				break;
			}
		}

		return new SearchResult(matches, hasMore);
	}

	// -- private helpers --------------------------------------------------------

	/**
	 * {@code app.log} plus any existing rotated siblings, newest-first -
	 * {@code app.log.1} is the most
	 * recently rotated file under Log4j2's default (ascending) fileIndex
	 * strategy, and the highest configured suffix is the oldest retained.
	 */
	private List<File> resolveLogFiles(String projectId, String projectName) {
		return AppLogManager.getLogFiles(projectId, projectName);
	}

	private static boolean matchesFilter(String line, String query, Set<String> levels) {
		if (!levels.isEmpty()) {
			boolean levelMatch = false;
			for (String level : levels) {
				if (line.startsWith("[" + level)) {
					levelMatch = true;
					break;
				}
			}
			if (!levelMatch) {
				return false;
			}
		}
		if (!query.isEmpty() && !line.toLowerCase().contains(query)) {
			return false;
		}
		return true;
	}

	private Set<String> parseLevels(String raw) {
		Set<String> levels = new HashSet<>();
		if (raw == null || raw.isBlank()) {
			return levels;
		}
		for (String level : raw.split(",")) {
			String trimmed = level.trim().toUpperCase();
			if (!trimmed.isEmpty()) {
				levels.add(trimmed);
			}
		}
		return levels;
	}

	private int clampLimit(long requested) {
		if (requested <= 0) {
			return DEFAULT_LIMIT;
		}
		return (int) Math.min(requested, MAX_LIMIT);
	}

	private int clampOffset(long requested) {
		if (requested <= 0) {
			return 0;
		}
		return (int) Math.min(requested, MAX_OFFSET);
	}

	private NounMetadata buildResult(List<String> lines, boolean hasMore) {
		Map<String, Object> result = new HashMap<>();
		result.put("lines", lines);
		result.put("hasMore", hasMore);
		return new NounMetadata(GSON.toJson(result), PixelDataType.JSON_OBJECT, PixelOperationType.LOGGING_DATA);
	}

	record SearchResult(List<String> lines, boolean hasMore) {
	}

	private Map<String, Object> getParamMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return new HashMap<>();
	}

	private static String getString(Map<String, Object> map, String key) {
		if (map == null || key == null) {
			return "";
		}
		Object val = map.get(key);
		return (val != null && !val.toString().isBlank()) ? val.toString().trim() : "";
	}

	private static long parseLong(String val, long defaultValue) {
		if (val == null || val.isBlank()) {
			return defaultValue;
		}
		try {
			return Long.parseLong(val.trim());
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}
}
