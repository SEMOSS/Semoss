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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Rehydrates a retained table into a frame owned by the caller's Insight.
 */
public class OpenAutomationResultReactor extends AbstractReactor {

	private static final String RESULT_ID_KEY = "resultId";
	private static final String VARIABLE_KEY = "variable";
	private static final long MAX_OPEN_ROWS = AutomationConstants.SAVED_RESULT_OPEN_MAX_ROWS;
	private static final long MAX_OPEN_BYTES = AutomationConstants.SAVED_RESULT_OPEN_MAX_BYTES;

	public OpenAutomationResultReactor() {
		this.keysToGet =
				new String[] { ReactorKeysEnum.PROJECT.getKey(), RESULT_ID_KEY, VARIABLE_KEY };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = getString(ReactorKeysEnum.PROJECT.getKey());
		String resultId = getString(RESULT_ID_KEY);
		String variable = getString(VARIABLE_KEY);
		projectId = AutomationProjectService.getViewableAutomationProject(this.insight.getUser(), projectId)
				.getProjectId();
		Map<String, Object> result = AutomationRunStore.getSavedResult(resultId);
		if (result == null || !projectId.equals(result.get(AutomationConstants.PROJECT_ID))) {
			throw new IllegalArgumentException("Automation result not found: " + resultId);
		}
		if (!AutomationConstants.RESULT_KIND_TABLE.equals(result.get(AutomationConstants.RESULT_KIND))) {
			throw new IllegalArgumentException("Only retained table results can be opened as a frame.");
		}
		long rowCount = ((Number) result.get(AutomationConstants.ROW_COUNT)).longValue();
		long byteCount = ((Number) result.get(AutomationConstants.BYTE_COUNT)).longValue();
		if (rowCount > MAX_OPEN_ROWS || byteCount > MAX_OPEN_BYTES) {
			throw new IllegalArgumentException("This retained table exceeds the open-as-frame policy of "
					+ MAX_OPEN_ROWS + " rows and " + MAX_OPEN_BYTES + " bytes; use GetAutomationResult paging.");
		}
		if (variable == null || variable.isBlank()) {
			variable = "automation_result_" + resultId.replace("-", "");
		}
		if (!variable.matches("[A-Za-z_][A-Za-z0-9_]*")) {
			throw new IllegalArgumentException("Frame variable must be a valid identifier.");
		}

		List<?> schema = AutomationRuntimeUtils.GSON.fromJson(
				String.valueOf(result.get(AutomationConstants.PAYLOAD_VALUE)), List.class);
		String[] headers = new String[schema.size()];
		String[] types = new String[schema.size()];
		for (int index = 0; index < schema.size(); index++) {
			Map<?, ?> column = (Map<?, ?>) schema.get(index);
			headers[index] = String.valueOf(column.get("name"));
			types[index] = String.valueOf(column.get("type"));
		}
		H2Frame frame = new H2Frame(headers, types);
		MessageDigest digest = digest();
		long inserted = 0;
		try (PreparedStatement statement = frame.createInsertPreparedStatement(headers)) {
			for (Map<String, Object> chunkMetadata : AutomationRunStore.getSavedResultChunkMetadata(resultId)) {
				int chunkIndex = ((Number) chunkMetadata.get(AutomationConstants.CHUNK_INDEX)).intValue();
				Map<String, Object> chunk = AutomationRunStore.getSavedResultChunk(resultId, chunkIndex);
				String payload = String.valueOf(chunk.get(AutomationConstants.CHUNK_VALUE));
				byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
				verifyChunk(chunk, bytes);
				digest.update(bytes);
				List<?> rows = AutomationRuntimeUtils.GSON.fromJson(payload, List.class);
				for (Object rawRow : rows) {
					List<?> row = (List<?>) rawRow;
					if (row.size() != headers.length) {
						throw new IllegalStateException("Retained Automation row does not match its schema.");
					}
					for (int column = 0; column < row.size(); column++) {
						setValue(statement, column + 1, types[column], row.get(column));
					}
					statement.addBatch();
					inserted++;
					if (inserted % 1_000 == 0) {
						statement.executeBatch();
					}
				}
			}
			statement.executeBatch();
			if (inserted != rowCount
					|| !String.valueOf(result.get(AutomationConstants.CHECKSUM))
							.equals(HexFormat.of().formatHex(digest.digest()))) {
				throw new IllegalStateException("Retained Automation table failed its integrity check.");
			}
		} catch (Exception e) {
			try {
				frame.close();
			} catch (Exception cleanupError) {
				e.addSuppressed(cleanupError);
			}
			throw new IllegalStateException("Unable to open the retained Automation table.", e);
		}

		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME,
				PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		this.insight.getVarStore().put(variable, noun);
		return noun;
	}

	private static void verifyChunk(Map<String, Object> chunk, byte[] bytes) {
		if (((Number) chunk.get(AutomationConstants.BYTE_COUNT)).longValue() != bytes.length
				|| !String.valueOf(chunk.get(AutomationConstants.CHECKSUM))
						.equals(HexFormat.of().formatHex(digest().digest(bytes)))) {
			throw new IllegalStateException("Retained Automation chunk failed its integrity check.");
		}
	}

	private static void setValue(PreparedStatement statement, int index, String type, Object value)
			throws SQLException {
		if (value == null) {
			statement.setObject(index, null);
		} else if ("DATE".equals(type)) {
			statement.setDate(index, java.sql.Date.valueOf(String.valueOf(value).substring(0, 10)));
		} else if ("TIMESTAMP".equals(type)) {
			statement.setTimestamp(index, timestamp(String.valueOf(value)));
		} else {
			statement.setObject(index, value);
		}
	}

	private static Timestamp timestamp(String value) {
		try {
			return Timestamp.from(Instant.parse(value));
		} catch (RuntimeException ignored) {
			try {
				return Timestamp.from(OffsetDateTime.parse(value).toInstant());
			} catch (RuntimeException noOffset) {
				return Timestamp.valueOf(LocalDateTime.parse(value));
			}
		}
	}

	private static MessageDigest digest() {
		try {
			return MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is unavailable.", e);
		}
	}
}
