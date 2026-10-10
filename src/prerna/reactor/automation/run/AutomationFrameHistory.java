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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * Persists immutable, paged snapshots of tabular Automation node output.
 *
 * <p>
 * Live frames remain owned by the execution Insight. This class streams one
 * coherent view into Automation-owned scheduler tables so history can be read
 * after that Insight is gone. A snapshot is hidden until every chunk and its
 * metadata commit together in the {@code AVAILABLE} state.
 */
final class AutomationFrameHistory {

	private static final Logger classLogger = LogManager.getLogger(AutomationFrameHistory.class);

	private static final String INSERT_DATA = """
			INSERT INTO AUTOMATION_RUN_DATA
			(DATA_REFERENCE_ID, RUN_ID, NODE_ID, OUTPUT_VAR_NAME, DATA_STATE, DATA_HEADERS, DATA_TYPES,
			 DATA_ROW_COUNT, DATA_COLUMN_COUNT, DATA_CONTENT_BYTES, DATA_CREATED_AT)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""";
	private static final String INSERT_CHUNK = """
			INSERT INTO AUTOMATION_RUN_DATA_CHUNKS
			(DATA_REFERENCE_ID, DATA_CHUNK_INDEX, DATA_ROW_OFFSET, DATA_CHUNK_ROW_COUNT, DATA_ROWS)
			VALUES (?, ?, ?, ?, ?)""";
	private static final String PUBLISH_DATA = """
			UPDATE AUTOMATION_RUN_DATA
			SET DATA_STATE = ?, DATA_ROW_COUNT = ?, DATA_CONTENT_BYTES = ?, DATA_AVAILABLE_AT = ?
			WHERE DATA_REFERENCE_ID = ? AND DATA_STATE = ?""";
	private static final String FIND_REFERENCE = """
			SELECT DATA_REFERENCE_ID FROM AUTOMATION_RUN_DATA WHERE RUN_ID = ? AND NODE_ID = ?""";
	private static final String DELETE_CHUNKS =
			"DELETE FROM AUTOMATION_RUN_DATA_CHUNKS WHERE DATA_REFERENCE_ID = ?";
	private static final String DELETE_DATA = "DELETE FROM AUTOMATION_RUN_DATA WHERE DATA_REFERENCE_ID = ?";
	private static final String SELECT_RUN_DATA = """
			SELECT DATA_REFERENCE_ID, RUN_ID, NODE_ID, OUTPUT_VAR_NAME, DATA_STATE, DATA_HEADERS, DATA_TYPES,
			       DATA_ROW_COUNT, DATA_COLUMN_COUNT, DATA_CONTENT_BYTES
			FROM AUTOMATION_RUN_DATA WHERE RUN_ID = ?""";
	private static final String SELECT_NODE_DATA = SELECT_RUN_DATA + " AND NODE_ID = ?";
	private static final String SELECT_CHUNKS = """
			SELECT DATA_CHUNK_INDEX, DATA_ROW_OFFSET, DATA_CHUNK_ROW_COUNT, DATA_ROWS
			FROM AUTOMATION_RUN_DATA_CHUNKS
			WHERE DATA_REFERENCE_ID = ? AND DATA_CHUNK_INDEX >= ? AND DATA_CHUNK_INDEX <= ?
			ORDER BY DATA_CHUNK_INDEX""";

	private AutomationFrameHistory() {
	}

	/** Captures an existing frame through the standard SEMOSS frame query contract. */
	static Snapshot captureFrame(String runId, String nodeId, String outputVariable, ITableDataFrame frame) {
		SelectQueryStruct queryStruct = new SelectQueryStruct();
		for (String header : frame.getQsHeaders()) {
			queryStruct.addSelector(new QueryColumnSelector(header));
		}
		queryStruct.setDistinct(false);
		try (IRawSelectWrapper wrapper = frame.query(queryStruct)) {
			return captureWrapper(runId, nodeId, outputVariable, wrapper);
		} catch (Exception e) {
			classLogger.error("Unable to retain frame output for Automation run '{}', node '{}'", runId, nodeId, e);
			throw new IllegalStateException("Unable to retain Automation frame history.", e);
		}
	}

	/**
	 * Streams a database task wrapper once, preserving duplicates, nulls, and source
	 * row order in the retained snapshot.
	 */
	static Snapshot captureWrapper(String runId, String nodeId, String outputVariable, IRawSelectWrapper wrapper) {
		String[] headers = wrapper.getHeaders();
		SemossDataType[] semossTypes = wrapper.getTypes();
		String[] types = Arrays.stream(semossTypes == null ? new SemossDataType[headers.length] : semossTypes)
				.map(type -> type == null ? null : type.toString()).toArray(String[]::new);
		String referenceId = UUID.randomUUID().toString();
		String headersJson = AutomationRuntimeUtils.GSON.toJson(headers);
		String typesJson = AutomationRuntimeUtils.GSON.toJson(types);
		IRDBMSEngine schedulerDb = requireSchedulerDb();

		try {
			return QueryExecutionUtility.write(schedulerDb, connection -> {
				deleteExisting(connection, schedulerDb, runId, nodeId);
				try (PreparedStatement insertData = connection.prepareStatement(INSERT_DATA)) {
					int index = 1;
					insertData.setString(index++, referenceId);
					insertData.setString(index++, runId);
					insertData.setString(index++, nodeId);
					schedulerDb.getQueryUtil().setNullableString(insertData, index++, outputVariable);
					insertData.setString(index++, AutomationConstants.DATA_STATE_WRITING);
					schedulerDb.getQueryUtil().setNullableLargeText(insertData, index++, headersJson);
					schedulerDb.getQueryUtil().setNullableLargeText(insertData, index++, typesJson);
					insertData.setLong(index++, 0L);
					insertData.setInt(index++, headers.length);
					insertData.setLong(index++, 0L);
					insertData.setTimestamp(index, Timestamp.from(Instant.now()));
					insertData.executeUpdate();
				}

				long rowCount = 0;
				long contentBytes = headersJson.getBytes(StandardCharsets.UTF_8).length
						+ typesJson.getBytes(StandardCharsets.UTF_8).length;
				int chunkIndex = 0;
				List<Object[]> rows = new ArrayList<>(AutomationConstants.RUN_DATA_CHUNK_SIZE);
				while (wrapper.hasNext()) {
					IHeadersDataRow row = wrapper.next();
					rows.add(row.getValues().clone());
					if (rows.size() == AutomationConstants.RUN_DATA_CHUNK_SIZE) {
						contentBytes += insertChunk(connection, schedulerDb, referenceId, chunkIndex++, rowCount, rows);
						rowCount += rows.size();
						rows.clear();
					}
				}
				if (!rows.isEmpty()) {
					contentBytes += insertChunk(connection, schedulerDb, referenceId, chunkIndex, rowCount, rows);
					rowCount += rows.size();
				}

				try (PreparedStatement publish = connection.prepareStatement(PUBLISH_DATA)) {
					publish.setString(1, AutomationConstants.DATA_STATE_AVAILABLE);
					publish.setLong(2, rowCount);
					publish.setLong(3, contentBytes);
					publish.setTimestamp(4, Timestamp.from(Instant.now()));
					publish.setString(5, referenceId);
					publish.setString(6, AutomationConstants.DATA_STATE_WRITING);
					if (publish.executeUpdate() != 1) {
						throw new IllegalStateException("Automation history snapshot was not published.");
					}
				}

				return new Snapshot(referenceId, runId, nodeId, outputVariable,
						AutomationConstants.DATA_STATE_AVAILABLE, List.of(headers), Arrays.asList(types), rowCount,
						headers.length, contentBytes);
			});
		} catch (Exception e) {
			classLogger.error("Unable to retain tabular output for Automation run '{}', node '{}'", runId, nodeId, e);
			throw new IllegalStateException("Unable to retain Automation tabular history.", e);
		}
	}

	/** Returns all published tabular references for one run, keyed by runtime node ID. */
	static Map<String, Snapshot> findAvailableByRun(String runId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb();
		try {
			List<Snapshot> snapshots = QueryExecutionUtility.queryList(schedulerDb, SELECT_RUN_DATA,
					statement -> statement.setString(1, runId), AutomationFrameHistory::mapSnapshot);
			Map<String, Snapshot> byNode = new LinkedHashMap<>();
			for (Snapshot snapshot : snapshots) {
				if (AutomationConstants.DATA_STATE_AVAILABLE.equals(snapshot.state())) {
					byNode.put(snapshot.nodeId(), snapshot);
				}
			}
			return byNode;
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read Automation frame history.", e);
		}
	}

	/** Returns one bounded historical page without requiring the execution Insight. */
	static Page page(String runId, String nodeId, long offset, int limit) {
		if (offset < 0 || limit < 1 || limit > AutomationConstants.RUN_DATA_MAX_PAGE_SIZE) {
			throw new IllegalArgumentException("Automation data page bounds are invalid.");
		}
		IRDBMSEngine schedulerDb = requireSchedulerDb();
		try {
			Snapshot snapshot = QueryExecutionUtility.queryOne(schedulerDb, SELECT_NODE_DATA, statement -> {
				statement.setString(1, runId);
				statement.setString(2, nodeId);
			}, AutomationFrameHistory::mapSnapshot);
			if (snapshot == null || !AutomationConstants.DATA_STATE_AVAILABLE.equals(snapshot.state())) {
				return null;
			}
			if (offset >= snapshot.rowCount()) {
				return new Page(snapshot, offset, List.of());
			}

			long lastExclusive = Math.min(snapshot.rowCount(), offset + limit);
			int firstChunk = Math.toIntExact(offset / AutomationConstants.RUN_DATA_CHUNK_SIZE);
			int lastChunk = Math.toIntExact((lastExclusive - 1) / AutomationConstants.RUN_DATA_CHUNK_SIZE);
			List<Chunk> chunks = QueryExecutionUtility.queryList(schedulerDb, SELECT_CHUNKS, statement -> {
				statement.setString(1, snapshot.referenceId());
				statement.setInt(2, firstChunk);
				statement.setInt(3, lastChunk);
			}, result -> new Chunk(result.getInt(1), result.getLong(2), result.getInt(3), result.getString(4)));

			List<List<Object>> pageRows = new ArrayList<>(limit);
			for (Chunk chunk : chunks) {
				Object[][] chunkRows = AutomationRuntimeUtils.GSON.fromJson(chunk.rowsJson(), Object[][].class);
				for (int index = 0; index < chunkRows.length; index++) {
					long rowOffset = chunk.rowOffset() + index;
					if (rowOffset >= offset && rowOffset < lastExclusive) {
						pageRows.add(Arrays.asList(chunkRows[index]));
					}
				}
			}
			return new Page(snapshot, offset, pageRows);
		} catch (Exception e) {
			throw new IllegalStateException("Unable to page Automation frame history.", e);
		}
	}

	private static long insertChunk(java.sql.Connection connection, IRDBMSEngine schedulerDb, String referenceId,
			int chunkIndex, long rowOffset, List<Object[]> rows) throws Exception {
		String json = AutomationRuntimeUtils.GSON.toJson(rows);
		try (PreparedStatement insert = connection.prepareStatement(INSERT_CHUNK)) {
			insert.setString(1, referenceId);
			insert.setInt(2, chunkIndex);
			insert.setLong(3, rowOffset);
			insert.setInt(4, rows.size());
			schedulerDb.getQueryUtil().setNullableLargeText(insert, 5, json);
			insert.executeUpdate();
		}
		return json.getBytes(StandardCharsets.UTF_8).length;
	}

	private static void deleteExisting(java.sql.Connection connection, IRDBMSEngine schedulerDb, String runId,
			String nodeId) throws Exception {
		String existing = QueryExecutionUtility.queryOne(connection, FIND_REFERENCE, statement -> {
			statement.setString(1, runId);
			statement.setString(2, nodeId);
		}, result -> result.getString(1));
		if (existing == null) {
			return;
		}
		try (PreparedStatement deleteChunks = connection.prepareStatement(DELETE_CHUNKS);
				PreparedStatement deleteData = connection.prepareStatement(DELETE_DATA)) {
			deleteChunks.setString(1, existing);
			deleteChunks.executeUpdate();
			deleteData.setString(1, existing);
			deleteData.executeUpdate();
		}
	}

	private static Snapshot mapSnapshot(ResultSet result) throws Exception {
		String[] headers = AutomationRuntimeUtils.GSON.fromJson(result.getString(6), String[].class);
		String[] types = AutomationRuntimeUtils.GSON.fromJson(result.getString(7), String[].class);
		return new Snapshot(result.getString(1), result.getString(2), result.getString(3), result.getString(4),
				result.getString(5), Arrays.asList(headers), Arrays.asList(types), result.getLong(8), result.getInt(9),
				result.getLong(10));
	}

	private static IRDBMSEngine requireSchedulerDb() {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		if (schedulerDb == null) {
			throw new IllegalStateException("Scheduler database is unavailable for Automation history.");
		}
		return schedulerDb;
	}

	record Snapshot(String referenceId, String runId, String nodeId, String outputVariable, String state,
			List<String> headers, List<String> types, long rowCount, int columnCount, long contentBytes) {
	}

	record Page(Snapshot snapshot, long offset, List<List<Object>> rows) {
	}

	private record Chunk(int index, long rowOffset, int rowCount, String rowsJson) {
	}
}
