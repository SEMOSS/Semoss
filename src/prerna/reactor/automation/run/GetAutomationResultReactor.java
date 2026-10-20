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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns a bounded page from a retained Automation result.
 */
public class GetAutomationResultReactor extends AbstractReactor {

	private static final String RESULT_ID_KEY = "resultId";
	private static final String OFFSET_KEY = "offset";
	private static final int DEFAULT_LIMIT = AutomationConstants.SAVED_RESULT_PAGE_DEFAULT_ROWS;
	private static final int MAX_LIMIT = AutomationConstants.SAVED_RESULT_PAGE_MAX_ROWS;
	private static final int MAX_PAGE_BYTES = AutomationConstants.SAVED_RESULT_PAGE_MAX_BYTES;

	public GetAutomationResultReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), RESULT_ID_KEY, OFFSET_KEY,
				ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = getString(ReactorKeysEnum.PROJECT.getKey());
		String resultId = getString(RESULT_ID_KEY);
		long offset = getLong(OFFSET_KEY, 0);
		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT);
		if (resultId == null || resultId.isBlank()) {
			throw new IllegalArgumentException("Must provide a result id");
		}
		if (offset < 0 || limit <= 0 || limit > MAX_LIMIT) {
			throw new IllegalArgumentException(
					"Result offset must be non-negative and limit must be between 1 and " + MAX_LIMIT + ".");
		}
		projectId = AutomationProjectService.getViewableAutomationProject(this.insight.getUser(), projectId)
				.getProjectId();
		Map<String, Object> stored = AutomationRunStore.getSavedResult(resultId);
		if (stored == null || !projectId.equals(stored.get(AutomationConstants.PROJECT_ID))) {
			throw new IllegalArgumentException("Automation result not found: " + resultId);
		}

		Map<String, Object> response = new LinkedHashMap<>(stored);
		Object payloadValue = response.remove(AutomationConstants.PAYLOAD_VALUE);
		if (!AutomationConstants.RESULT_KIND_TABLE.equals(stored.get(AutomationConstants.RESULT_KIND))) {
			throw new IllegalStateException("Unsupported Automation result kind.");
		}
		response.put("schema", AutomationRuntimeUtils.GSON.fromJson(String.valueOf(payloadValue), List.class));
		long rowCount = ((Number) stored.get(AutomationConstants.ROW_COUNT)).longValue();
		if (offset > rowCount) {
			throw new IllegalArgumentException("Result offset exceeds the retained row count.");
		}

		List<Object> rows = new ArrayList<>(limit);
		int pageBytes = 2;
		long requestedEnd = Math.min(rowCount, offset + limit);
		for (Map<String, Object> metadata : AutomationRunStore.getSavedResultChunkMetadata(resultId)) {
			long first = ((Number) metadata.get(AutomationConstants.FIRST_ROW_INDEX)).longValue();
			long count = ((Number) metadata.get(AutomationConstants.CHUNK_ROW_COUNT)).longValue();
			if (first >= requestedEnd || first + count <= offset) {
				continue;
			}
			int chunkIndex = ((Number) metadata.get(AutomationConstants.CHUNK_INDEX)).intValue();
			Map<String, Object> chunk = AutomationRunStore.getSavedResultChunk(resultId, chunkIndex);
			String payload = String.valueOf(chunk.get(AutomationConstants.CHUNK_VALUE));
			verifyChunk(payload, chunk);
			List<?> chunkRows = AutomationRuntimeUtils.GSON.fromJson(payload, List.class);
			int from = (int) Math.max(0, offset - first);
			int to = (int) Math.min(chunkRows.size(), requestedEnd - first);
			for (int index = from; index < to; index++) {
				Object row = chunkRows.get(index);
				int rowBytes = AutomationRuntimeUtils.toRuntimeJson(row).getBytes(StandardCharsets.UTF_8).length + 1;
				if (pageBytes + rowBytes > MAX_PAGE_BYTES) {
					requestedEnd = offset + rows.size();
					break;
				}
				rows.add(row);
				pageBytes += rowBytes;
			}
			if (offset + rows.size() < Math.min(rowCount, offset + limit)) {
				break;
			}
		}
		long nextOffset = offset + rows.size();
		response.put("rows", rows);
		response.put("offset", offset);
		response.put("nextOffset", nextOffset);
		response.put("hasMore", nextOffset < rowCount);
		return noun(response);
	}

	private static void verifyChunk(String payload, Map<String, Object> chunk) {
		byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
		if (((Number) chunk.get(AutomationConstants.BYTE_COUNT)).longValue() != bytes.length
				|| !String.valueOf(chunk.get(AutomationConstants.CHECKSUM)).equals(sha256(bytes))) {
			throw new IllegalStateException("Automation result chunk failed its integrity check.");
		}
	}

	private static String sha256(byte[] value) {
		try {
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is unavailable.", e);
		}
	}

	private static NounMetadata noun(Map<String, Object> value) {
		return new NounMetadata(value, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private long getLong(String key, long defaultValue) {
		String value = getString(key);
		return value == null || value.isBlank() ? defaultValue : Long.parseLong(value);
	}
}
