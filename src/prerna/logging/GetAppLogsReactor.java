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
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
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
 * Reads new lines from the per-project log file since a given byte offset.
 * Designed to be polled every few seconds by the semoss-ui AppLogsPage terminal.
 * <p>
 * Security: requires the calling user to be the project owner. Anonymous users,
 * non-owners, and unauthenticated requests are all rejected.
 * <p>
 * Pixel usage:
 * <pre>
 * // Initial load — pass offset=-1 to get the last ~50 KB of history
 * GetAppLogs(paramValues=[{"projectId": "X", "offset": "-1"}]);
 *
 * // Subsequent polls — pass the nextOffset returned by the previous call
 * GetAppLogs(paramValues=[{"projectId": "X", "offset": "12345"}]);
 * </pre>
 * Returns:
 * <pre>
 * {
 *   "lines":      ["[INFO ] ...", "[WARN ] ..."],  // new lines since offset
 *   "nextOffset": 12345,                           // pass this on the next call
 *   "fileSize":   12345                            // current file size; if
 *                                                  // nextOffset > fileSize on
 *                                                  // next call, file was rotated
 * }
 * </pre>
 */
public class GetAppLogsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetAppLogsReactor.class);

	/** Bytes of history returned on the first call (offset=-1). ~50 KB. */
	private static final long INITIAL_HISTORY_BYTES = 51_200L;

	public GetAppLogsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// ── Auth ────────────────────────────────────────────────────────────

		User user = this.insight.getUser();

		// Reject anonymous users
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		Map<String, Object> params = getParamMap();
		String projectId = getString(params, SemossLogUtils.PROJECT_ID);
		if (projectId == null || projectId.isBlank()) {
			throw new IllegalArgumentException("Must provide 'projectId'");
		}

		// Only project owners can read app logs — non-owners could expose
		// other users' activity and sensitive request/response payloads
		String userId = user.getPrimaryLoginToken().getId();
		Integer permissionLvl = SecurityProjectUtils.getUserProjectPermission(userId, projectId);
		if (permissionLvl == null && !SecurityProjectUtils.projectIsGlobal(projectId)) {
			throw new IllegalArgumentException(
					"Project '" + projectId + "' does not exist or user does not have access");
		}
		if (permissionLvl == null || !AccessPermissionEnum.isOwner(permissionLvl)) {
			throw new IllegalArgumentException("Only project owners can read app logs");
		}

		// ── Resolve log file ─────────────────────────────────────────────────

		String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
		String logFilePath = AppLogManager.getLogFilePath(projectId, projectName);
		File logFile = new File(logFilePath);

		if (!logFile.exists() || !logFile.isFile()) {
			// App hasn't been loaded since the feature shipped — no log yet
			return buildResult(Collections.emptyList(), 0L, 0L);
		}

		// ── Read new lines ───────────────────────────────────────────────────

		long fileSize = logFile.length();
		long rawOffset = parseLong(getString(params, "offset"), -1L);
		// -1 = initial load: start from last INITIAL_HISTORY_BYTES of the file
		// If offset > fileSize the file was rotated — reset to start
		long startPos = resolveOffset(rawOffset, fileSize);

		if (startPos >= fileSize) {
			// No new content since last poll
			return buildResult(Collections.emptyList(), fileSize, fileSize);
		}

		List<String> lines = readLines(logFile, startPos, fileSize);
		// nextOffset = fileSize at time of read — Log4j2 writes each event atomically
		// so we never split a log line across polls
		return buildResult(lines, fileSize, fileSize);
	}

	// ── private helpers ──────────────────────────────────────────────────────

	/**
	 * Resolves the client-supplied offset to a valid byte position in the file.
	 *
	 * @param rawOffset the offset param from the client (-1 for initial load)
	 * @param fileSize  current size of the log file in bytes
	 * @return byte position to start reading from
	 */
	private static long resolveOffset(long rawOffset, long fileSize) {
		if (rawOffset < 0) {
			// Initial load: last INITIAL_HISTORY_BYTES of the file
			return Math.max(0L, fileSize - INITIAL_HISTORY_BYTES);
		}
		if (rawOffset > fileSize) {
			// File was rotated/truncated — start from the beginning
			classLogger.debug("Log offset {} > fileSize {} — log was rotated, resetting to 0",
					rawOffset, fileSize);
			return 0L;
		}
		return rawOffset;
	}

	/**
	 * Reads all complete lines from {@code startPos} to end of file.
	 * Uses {@link FileInputStream} + {@link java.io.InputStreamReader} for
	 * correct UTF-8 decoding of multi-byte characters.
	 */
	private static List<String> readLines(File logFile, long startPos, long fileSize) {
		int bytesToRead = (int) (fileSize - startPos);
		if (bytesToRead <= 0) {
			return Collections.emptyList();
		}
		byte[] buffer = new byte[bytesToRead];
		try (FileInputStream fis = new FileInputStream(logFile)) {
			fis.getChannel().position(startPos);
			int read = fis.readNBytes(buffer, 0, bytesToRead);
			if (read <= 0) {
				return Collections.emptyList();
			}
			String chunk = new String(buffer, 0, read, StandardCharsets.UTF_8);
			return Arrays.stream(chunk.split("\n", -1))
					.filter(line -> !line.isEmpty())
					.collect(Collectors.toList());
		} catch (IOException e) {
			classLogger.error("Error reading log file '{}' from offset {}: {}",
					logFile.getPath(), startPos, e.getMessage(), e);
			return Collections.emptyList();
		}
	}

	private NounMetadata buildResult(List<String> lines, long nextOffset, long fileSize) {
		Map<String, Object> result = new HashMap<>();
		result.put("lines", lines);
		result.put("nextOffset", nextOffset);
		result.put("fileSize", fileSize);
		return new NounMetadata(GSON.toJson(result), PixelDataType.JSON_OBJECT,
				PixelOperationType.LOGGING_DATA);
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
		return (val != null && !StringUtils.isBlank(val.toString())) ? val.toString().trim() : "";
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
