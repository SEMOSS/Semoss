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

import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/**
 * Creates immutable retained-result chunks from Java tables.
 */
public final class AutomationTableCaptureService {

	static final int MAX_CHUNK_ROWS = AutomationConstants.SAVED_RESULT_MAX_CHUNK_ROWS;
	static final int MAX_CHUNK_BYTES = AutomationConstants.SAVED_RESULT_MAX_CHUNK_BYTES;
	static final long MAX_RESULT_ROWS = AutomationConstants.SAVED_RESULT_MAX_ROWS;
	static final long MAX_RESULT_BYTES = AutomationConstants.SAVED_RESULT_MAX_BYTES;
	static final int FORMAT_VERSION = AutomationConstants.SAVED_RESULT_FORMAT_VERSION;

	private AutomationTableCaptureService() {
	}


	static TableCapture capture(AutomationTableValue table, String nodeId) {
		List<TableChunk> chunks = new ArrayList<>();
		List<List<Object>> chunkRows = new ArrayList<>(MAX_CHUNK_ROWS);
		MessageDigest resultDigest = digest();
		long byteCount = 0;
		long firstRow = 0;
		for (List<Object> row : table.rows()) {
			byte[] singleRow = AutomationRuntimeUtils.toRuntimeJson(List.of(row)).getBytes(StandardCharsets.UTF_8);
			if (singleRow.length > MAX_CHUNK_BYTES) {
				throw new IllegalStateException(
						"Automation node '" + nodeId + "' produced a row larger than the saved-result chunk limit.");
			}
			chunkRows.add(row);
			byte[] candidate = AutomationRuntimeUtils.toRuntimeJson(chunkRows).getBytes(StandardCharsets.UTF_8);
			if (candidate.length > MAX_CHUNK_BYTES || chunkRows.size() > MAX_CHUNK_ROWS) {
				chunkRows.remove(chunkRows.size() - 1);
				byte[] payload = AutomationRuntimeUtils.toRuntimeJson(chunkRows).getBytes(StandardCharsets.UTF_8);
				byteCount = addChunk(chunks, resultDigest, firstRow, chunkRows.size(), payload, byteCount, nodeId);
				firstRow += chunkRows.size();
				chunkRows = new ArrayList<>(MAX_CHUNK_ROWS);
				chunkRows.add(row);
			}
		}
		if (!chunkRows.isEmpty()) {
			byte[] payload = AutomationRuntimeUtils.toRuntimeJson(chunkRows).getBytes(StandardCharsets.UTF_8);
			byteCount = addChunk(chunks, resultDigest, firstRow, chunkRows.size(), payload, byteCount, nodeId);
		}
		String schema = AutomationRuntimeUtils.toRuntimeJson(
				table.columns().stream().map(AutomationTableValue.Column::asMap).toList());
		return new TableCapture(schema, List.copyOf(chunks), table.rowCount(), byteCount,
				HexFormat.of().formatHex(resultDigest.digest()));
	}

	static List<Object> page(TableCapture capture, long offset, int limit) {
		if (offset < 0 || limit < 0 || offset + limit > capture.rowCount()) {
			throw new IllegalArgumentException("Saved table page is outside the captured row range.");
		}
		List<?> rawSchema = AutomationRuntimeUtils.GSON.fromJson(capture.schema(), List.class);
		List<String> headers = new ArrayList<>(rawSchema.size());
		for (Object rawColumn : rawSchema) {
			headers.add(String.valueOf(((Map<?, ?>) rawColumn).get("name")));
		}
		List<Object> page = new ArrayList<>(limit);
		long end = offset + limit;
		for (TableChunk chunk : capture.chunks()) {
			if (chunk.firstRow() >= end || chunk.firstRow() + chunk.rowCount() <= offset) {
				continue;
			}
			List<?> rows = AutomationRuntimeUtils.GSON.fromJson(chunk.payload(), List.class);
			int from = (int) Math.max(0, offset - chunk.firstRow());
			int to = (int) Math.min(rows.size(), end - chunk.firstRow());
			for (int rowIndex = from; rowIndex < to; rowIndex++) {
				List<?> values = (List<?>) rows.get(rowIndex);
				Map<String, Object> row = new LinkedHashMap<>();
				for (int column = 0; column < headers.size(); column++) {
					row.put(headers.get(column), values.get(column));
				}
				page.add(row);
			}
		}
		if (page.size() != limit) {
			throw new IllegalStateException("Captured table page is incomplete.");
		}
		return page;
	}

	static Map<String, Object> promptValue(TableCapture capture) {
		int includedRows = Math.toIntExact(Math.min(capture.rowCount(), 100));
		Map<String, Object> value = new LinkedHashMap<>();
		value.put("dataType", "table");
		value.put("rowCount", capture.rowCount());
		value.put("rows", page(capture, 0, includedRows));
		value.put("truncated", capture.rowCount() > includedRows);
		return value;
	}

	static Map<String, Object> summary(TableCapture capture) {
		List<?> schema = AutomationRuntimeUtils.GSON.fromJson(capture.schema(), List.class);
		Map<String, Object> value = new LinkedHashMap<>();
		value.put("dataType", "table");
		value.put("rowCount", capture.rowCount());
		value.put("columnCount", schema.size());
		return value;
	}

	private static long addChunk(List<TableChunk> chunks, MessageDigest resultDigest, long firstRow, int rowCount,
			byte[] payload, long currentBytes, String nodeId) {
		long total = currentBytes + payload.length;
		if (total > MAX_RESULT_BYTES) {
			throw new IllegalStateException(
					"Automation node '" + nodeId + "' exceeded the saved-result byte limit.");
		}
		resultDigest.update(payload);
		chunks.add(new TableChunk(chunks.size(), firstRow, rowCount, new String(payload, StandardCharsets.UTF_8),
				payload.length, sha256(payload)));
		return total;
	}

	private static MessageDigest digest() {
		try {
			return MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is unavailable.", e);
		}
	}

	private static String sha256(byte[] value) {
		return HexFormat.of().formatHex(digest().digest(value));
	}

	public record TableCapture(String schema, List<TableChunk> chunks, long rowCount, long byteCount, String checksum) {
	}

	public record TableChunk(int index, long firstRow, long rowCount, String payload, long byteCount, String checksum) {
	}
}
