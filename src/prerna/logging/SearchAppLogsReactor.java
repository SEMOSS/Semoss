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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
 * {@code app.log}'s own rotation (50MB x 10 files, see {@link AppLogManager})
 * already bounds how much there is to search.
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
 * Returns {@code {"lines": [...], "totalMatches": N, "hasMore": bool}} - lines
 * are newest-first.
 * <p>
 * Security: owner-only, same rule {@code InsightWebsocket}'s app_logs watch
 * gate uses - logs can expose request/response payloads and other users'
 * activity.
 */
public class SearchAppLogsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SearchAppLogsReactor.class);

	private static final int DEFAULT_LIMIT = 50;
	private static final int MAX_LIMIT = 500;
	/** Rotated file suffixes to search, newest-rotated first - mirrors AppLogManager's DefaultRolloverStrategy numbering. */
	private static final int MAX_ROTATED_FILES = 10;

	public SearchAppLogsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

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
		int offset = Math.max(0, (int) parseLong(getString(params, "offset"), 0L));
		int limit = clampLimit(parseLong(getString(params, "limit"), DEFAULT_LIMIT));

		String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
		List<File> files = resolveLogFiles(projectId, projectName);

		List<String> matches = new ArrayList<>();
		int totalMatches = 0;
		// Newest file first, and within each file, newest line first - scan
		// everything to get an accurate total, but stop building `matches` once
		// we've collected enough for this page (offset + limit).
		for (File file : files) {
			List<String> lines = readLinesReversed(file);
			for (String line : lines) {
				if (!matchesFilter(line, query, levels)) {
					continue;
				}
				if (totalMatches >= offset && matches.size() < limit) {
					matches.add(line);
				}
				totalMatches++;
			}
		}

		return buildResult(matches, totalMatches, offset + matches.size() < totalMatches);
	}

	// -- private helpers --------------------------------------------------------

	/**
	 * {@code app.log} plus any existing rotated siblings ({@code app.log.1} ...
	 * {@code app.log.10}), newest-first - {@code app.log.1} is the most
	 * recently rotated file under Log4j2's default (ascending) fileIndex
	 * strategy, {@code .10} the oldest still retained.
	 */
	private List<File> resolveLogFiles(String projectId, String projectName) {
		List<File> files = new ArrayList<>();
		String basePath = AppLogManager.getLogFilePath(projectId, projectName);
		File active = new File(basePath);
		if (active.exists()) {
			files.add(active);
		}
		for (int i = 1; i <= MAX_ROTATED_FILES; i++) {
			File rotated = new File(basePath + "." + i);
			if (rotated.exists()) {
				files.add(rotated);
			}
		}
		return files;
	}

	/** Reads a file and returns its lines newest-first (last line in the file first). */
	private List<String> readLinesReversed(File file) {
		List<String> lines = new ArrayList<>();
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}
		} catch (IOException e) {
			classLogger.warn("Could not read log file '{}': {}", file.getPath(), e.getMessage());
			return lines;
		}
		java.util.Collections.reverse(lines);
		return lines;
	}

	private boolean matchesFilter(String line, String query, Set<String> levels) {
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

	private NounMetadata buildResult(List<String> lines, int totalMatches, boolean hasMore) {
		Map<String, Object> result = new HashMap<>();
		result.put("lines", lines);
		result.put("totalMatches", totalMatches);
		result.put("hasMore", hasMore);
		return new NounMetadata(GSON.toJson(result), PixelDataType.JSON_OBJECT, PixelOperationType.LOGGING_DATA);
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
