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
package prerna.engine.logging;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.logging.LogActivityRecord;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class AuditLogsDbUtils {

	private static final Logger classLogger = LogManager.getLogger(AuditLogsDbUtils.class);

	/**
	 * Sentinel roomId value meaning "only logs where ROOM_ID IS NULL" (activity not
	 * tied to any room)
	 */
	public static final String NULL_ROOM_ID = "null";

	static boolean initialized = false;

	private AuditLogsDbUtils() {

	}

	public static void loadAuditLogsDatabase() throws Exception {
		IRDBMSEngine auditLogsDb = SystemEngineRegistry.getAuditLogsDb();
		initEngineAsAuditDatabase(auditLogsDb);
		initialized = true;
	}

	/**
	 * @param engine
	 * @param conn
	 * @param columnNamesAndTypes
	 * @throws SQLException
	 */
	private static void executeInitDatabaseSchema(IRDBMSEngine auditLogsDb, Connection conn,
			List<Pair<String, List<Pair<String, String>>>> dbSchema) throws SQLException {

		String database = auditLogsDb.getDatabase();
		String schema = auditLogsDb.getSchema();

		AbstractSqlQueryUtil queryUtil = auditLogsDb.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();

		for (Pair<String, List<Pair<String, String>>> tableSchema : dbSchema) {
			String tableName = tableSchema.getValue0();
			String[] colNames = tableSchema.getValue1().stream().map(Pair::getValue0).toArray(String[]::new);
			String[] types = tableSchema.getValue1().stream().map(Pair::getValue1).toArray(String[]::new);
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists(tableName, colNames, types);
				executeSql(conn, sql);
			} else {
				if (!queryUtil.tableExists(auditLogsDb, tableName, database, schema)) {
					String sql = queryUtil.createTable(tableName, colNames, types);
					executeSql(conn, sql);
				}
			}

			List<String> allCols = queryUtil.getTableColumns(conn, tableName, database, schema);
			for (int i = 0; i < colNames.length; i++) {
				String col = colNames[i];
				if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
					String addColumnSql = queryUtil.alterTableAddColumn(tableName, col, types[i]);
					executeSql(conn, addColumnSql);
				}
			}
		}
		if (allowIfExistsIndexs) {
			String sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__REQUEST_ID_INDEX", "AUDIT_LOGS", "REQUEST_ID");
			classLogger.info("Running sql " + sql);
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__PROJECT_TS_INDEX", "AUDIT_LOGS",
					List.of("PROJECT_ID", "LOG_TIMESTAMP"));
			classLogger.info("Running sql " + sql);
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__USER_TS_INDEX", "AUDIT_LOGS",
					List.of("USER_ID", "LOG_TIMESTAMP"));
			classLogger.info("Running sql " + sql);
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__ENGINE_TS_INDEX", "AUDIT_LOGS",
					List.of("ENGINE_ID", "LOG_TIMESTAMP"));
			classLogger.info("Running sql " + sql);
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__SESSION_ID_INDEX", "AUDIT_LOGS", "SESSION_ID");
			classLogger.info("Running sql " + sql);
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__ROOM_ID_INDEX", "AUDIT_LOGS", "ROOM_ID");
			classLogger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// REQUEST_ID
			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__REQUEST_ID_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__REQUEST_ID_INDEX", "AUDIT_LOGS", "REQUEST_ID");
				classLogger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
			// COMPOSITE INDEX PROJECT_ID + LOG_TIMESTAMP
			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__PROJECT_TS_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__PROJECT_TS_INDEX", "AUDIT_LOGS",
						List.of("PROJECT_ID", "LOG_TIMESTAMP"));
				classLogger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
			// COMPOSITE INDEX USER_ID + LOG_TIMESTAMP
			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__USER_TS_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__USER_TS_INDEX", "AUDIT_LOGS",
						List.of("USER_ID", "LOG_TIMESTAMP"));
				classLogger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
			// COMPOSITE INDEX ENGINE_ID + LOG_TIMESTAMP
			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__ENGINE_TS_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__ENGINE_TS_INDEX", "AUDIT_LOGS",
						List.of("ENGINE_ID", "LOG_TIMESTAMP"));
				classLogger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
			// SESSION_ID
			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__SESSION_ID_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__SESSION_ID_INDEX", "AUDIT_LOGS", "SESSION_ID");
				classLogger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
			// ROOM_ID
			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__ROOM_ID_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__ROOM_ID_INDEX", "AUDIT_LOGS", "ROOM_ID");
				classLogger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}
	}

	/**
	 * @param conn
	 * @param sql
	 * @throws SQLException
	 */
	private static void executeSql(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			classLogger.info("Running sql " + sql);
			stmt.execute(sql);
		}
	}

	/**
	 * 
	 * @return
	 */
	public static boolean isInitalized() {
		return initialized;
	}

	/**
	 * Transform any RDBMS engine into an audit logs database
	 * 
	 * @param auditLogsDb
	 * @throws Exception
	 */
	public static synchronized void initEngineAsAuditDatabase(IRDBMSEngine auditLogsDb) throws Exception {
		AuditLogsDbOwlCreator owlCreator = new AuditLogsDbOwlCreator(auditLogsDb);
		if (owlCreator.needsRemake()) {
			owlCreator.remakeOwl();
		}

		Connection conn = null;
		try {
			conn = auditLogsDb.getConnection();
			executeInitDatabaseSchema(auditLogsDb, conn, owlCreator.getDBSchema());
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(auditLogsDb, conn, null, null);
		}
	}

	/**
	 * 
	 * @param userId
	 * @param projectId
	 * @param engineId
	 * @param dateTime
	 * @param roomId
	 * @param sessionId
	 * @param offset
	 * @param limit
	 * @return
	 * @throws SQLException
	 */
	public static List<LogActivityRecord> getAuditLogsTimeLineData(String userId, String projectId, String engineId,
			SemossDate startDate, SemossDate endDate, String roomId, String sessionId, int limit, int offset,
			List<String> methodNames, List<String> engineTypes, String searchTerm) throws SQLException {
		IRDBMSEngine auditLogsDb = SystemEngineRegistry.getAuditLogsDb();
		AbstractSqlQueryUtil queryUtil = auditLogsDb.getQueryUtil();

		// step 1: fetch the page of rows. No subquery join - aggregation is deferred
		// to step 2 so it only runs over the REQUEST_IDs we actually return.
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__REQUEST_ID"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__ENGINE_NAME"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__ENGINE_TYPE"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__METHOD_NAME"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__REQUEST"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__RESPONSE"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__NUMBER_OF_TOKENS_IN_PROMPT"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__NUMBER_OF_TOKENS_IN_RESPONSE"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__IS_SUCCESS"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__USER_NAME"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__USER_ID"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__SESSION_ID"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__SPAN_ID"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__LOG_TIMESTAMP"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__REQUEST_START_TIME"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__RESPONSE_END_TIME"));

		addStartDateEndDateFitler(qs, "AUDIT_LOGS__LOG_TIMESTAMP", startDate, endDate);
		addFilter(qs, "AUDIT_LOGS__USER_ID", "==", userId);
		addFilter(qs, "AUDIT_LOGS__PROJECT_ID", "==", projectId);
		addFilter(qs, "AUDIT_LOGS__ENGINE_ID", "==", engineId);
		addRoomIdFilter(qs, roomId);
		addFilter(qs, "AUDIT_LOGS__SESSION_ID", "==", sessionId);
		addMultiValueFilter(qs, "AUDIT_LOGS__METHOD_NAME", methodNames);
		addMultiValueFilter(qs, "AUDIT_LOGS__ENGINE_TYPE", engineTypes);
		addGlobalSearchFilter(queryUtil, qs, searchTerm);

		qs.addOrderBy("AUDIT_LOGS__LOG_TIMESTAMP", "desc");
		// pagination
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}

		List<Map<String, Object>> pageRows = QueryExecutionUtility.flushRsToMap(auditLogsDb, qs);
		if (pageRows.isEmpty()) {
			return Collections.emptyList();
		}

		List<LogActivityRecord> activityList = new ArrayList<>(pageRows.size());
		for (Map<String, Object> map : pageRows) {
			String requestId = getOrDefault(map.get("REQUEST_ID"), "");
			// each row carries its own operation timing - do NOT aggregate across the
			// request, otherwise every row (input guardrail, model call, output
			// guardrail) would show the same request-wide start/end/latency
			String startTime = toUtcIso(map.get("REQUEST_START_TIME"));
			String endTime = toUtcIso(map.get("RESPONSE_END_TIME"));
			long latency = latencyMillis(map.get("REQUEST_START_TIME"), map.get("RESPONSE_END_TIME"));

			String request = getOrDefault(map.get("REQUEST"), "");
			String response = getOrDefault(map.get("RESPONSE"), "");
			String engineName = getOrDefault(map.get("ENGINE_NAME"), null);
			String engineType = getOrDefault(map.get("ENGINE_TYPE"), null);
			boolean status = map.get("IS_SUCCESS") instanceof Boolean && (Boolean) map.get("IS_SUCCESS");
			int tokens = getIntValue(map.get("NUMBER_OF_TOKENS_IN_PROMPT"))
					+ getIntValue(map.get("NUMBER_OF_TOKENS_IN_RESPONSE"));
			String methodName = getOrDefault(map.get("METHOD_NAME"), "");
			String userNameFromRow = getOrDefault(map.get("USER_NAME"), null);
			String userIdFromRow = getOrDefault(map.get("USER_ID"), null);
			String sessionIdFromRow = getOrDefault(map.get("SESSION_ID"), null);
			String spanIdFromRow = getOrDefault(map.get("SPAN_ID"), null);
			String logTimestamp = toUtcIso(map.get("LOG_TIMESTAMP"));

			activityList.add(new LogActivityRecord(requestId, startTime, endTime, request, response, tokens, latency,
					status, engineName, engineType, methodName, userNameFromRow, userIdFromRow, sessionIdFromRow,
					spanIdFromRow, logTimestamp));

		}
		return activityList;
	}

	/**
	 * Latency in milliseconds for a single row, computed from its own raw
	 * REQUEST_START_TIME and RESPONSE_END_TIME. Returns 0 if either is missing or
	 * the end precedes the start.
	 *
	 * @param startObj the row's REQUEST_START_TIME value
	 * @param endObj   the row's RESPONSE_END_TIME value
	 * @return latency in milliseconds (>= 0)
	 */
	private static long latencyMillis(Object startObj, Object endObj) {
		if (startObj instanceof SemossDate && endObj instanceof SemossDate) {
			Timestamp start = Utility.getSqlTimestampUTC((SemossDate) startObj);
			Timestamp end = Utility.getSqlTimestampUTC((SemossDate) endObj);
			if (start != null && end != null) {
				long millis = end.getTime() - start.getTime();
				return millis > 0 ? millis : 0L;
			}
		}
		return 0L;
	}

	// Helper Methods

	/**
	 * @param qs
	 * @param startDate
	 * @param endDate
	 */
	private static void addStartDateEndDateFitler(SelectQueryStruct qs, String column, SemossDate startDate,
			SemossDate endDate) {
		if (startDate != null) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(column, ">=", startDate));
		}
		if (endDate != null) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(column, "<=", endDate));
		}
	}

	/**
	 * 
	 * @param qs
	 * @param column
	 * @param operator
	 * @param value
	 */
	private static void addFilter(SelectQueryStruct qs, String column, String operator, String value) {
		if (value != null && !(value = value.trim()).isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(column, operator, value));
		}
	}

	/**
	 * Filter on ROOM_ID. The sentinel {@link #NULL_ROOM_ID} ("null") means "only
	 * rows where ROOM_ID IS NULL" - i.e. activity not tied to any room. A real id
	 * filters by equality; a blank/absent value applies no room filter (all rows).
	 *
	 * @param qs     the query struct to add the filter to
	 * @param roomId the requested room scope (real id, the "null" sentinel, or
	 *               blank)
	 */
	private static void addRoomIdFilter(SelectQueryStruct qs, String roomId) {
		if (roomId == null || (roomId = roomId.trim()).isEmpty()) {
			return;
		}
		if (NULL_ROOM_ID.equalsIgnoreCase(roomId)) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("AUDIT_LOGS__ROOM_ID", "==", null));
		} else {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("AUDIT_LOGS__ROOM_ID", "==", roomId));
		}
	}

	/**
	 * 
	 * @param qs
	 * @param column
	 * @param values
	 */
	private static void addMultiValueFilter(SelectQueryStruct qs, String column, List<String> values) {
		if (values == null || values.isEmpty()) {
			return;
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(column, "==", values));
	}

	/**
	 *
	 * @param qs
	 * @param value
	 */
	private static void addGlobalSearchFilter(AbstractSqlQueryUtil queryUtil, SelectQueryStruct qs, String value) {
		if (value == null || (value = value.trim()).isEmpty()) {
			return;
		}
		// Search across the human-readable VARCHAR columns. REQUEST and RESPONSE are
		// CLOB columns and are intentionally excluded - searching/filtering on CLOB
		// columns is not portable across databases and cannot use an index.
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(queryUtil.getSearchRegexFilter("AUDIT_LOGS__USER_NAME", value));
		orFilter.addFilter(queryUtil.getSearchRegexFilter("AUDIT_LOGS__METHOD_NAME", value));
		orFilter.addFilter(queryUtil.getSearchRegexFilter("AUDIT_LOGS__ENGINE_NAME", value));

		qs.addExplicitFilter(orFilter);
	}

	/**
	 * Format an audit log timestamp value as an ISO-8601 UTC instant string with
	 * millisecond precision (e.g. {@code 2026-06-22T17:48:07.123Z}) - unambiguous,
	 * sortable and parseable by clients, unlike a locale-formatted Timestamp.
	 *
	 * @param dateObj the raw column value (a {@link SemossDate})
	 * @return the ISO-8601 UTC string, or null if not a date
	 */
	private static String toUtcIso(Object dateObj) {
		if (dateObj instanceof SemossDate) {
			Timestamp ts = Utility.getSqlTimestampUTC((SemossDate) dateObj);
			if (ts != null) {
				return DateTimeFormatter.ISO_INSTANT.format(ts.toInstant().truncatedTo(ChronoUnit.MILLIS));
			}
		}
		return null;
	}

	/**
	 * 
	 * @param obj
	 * @param defaultValue
	 * @return
	 */
	private static String getOrDefault(Object obj, String defaultValue) {
		return (obj != null && !obj.toString().isEmpty()) ? obj.toString() : defaultValue;
	}

	/**
	 * 
	 * @param obj
	 * @return
	 */
	private static int getIntValue(Object obj) {
		return (obj instanceof Integer) ? (Integer) obj : 0;
	}

	/**
	 * Get audit log total record count
	 * 
	 * @param userId
	 * @param projectId
	 * @param engineId
	 * @param dateTime
	 * @param roomId
	 * @param sessionId
	 * @return
	 */
	public static long getAuditLogsCount(String userId, String projectId, String engineId, SemossDate startDate,
			SemossDate endDate, String roomId, String sessionId, List<String> methodNames, List<String> engineTypes,
			String searchTerm) {
		IRDBMSEngine auditLogsDb = SystemEngineRegistry.getAuditLogsDb();
		AbstractSqlQueryUtil queryUtil = auditLogsDb.getQueryUtil();

		SelectQueryStruct qs = new SelectQueryStruct();

		// COUNT(AUDIT_LOGS__LOG_ID) selector
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
		fSelector.setAlias("total_count");
		fSelector.setFunction(QueryFunctionHelper.COUNT);
		fSelector.addInnerSelector(new QueryColumnSelector("AUDIT_LOGS__LOG_ID"));
		qs.addSelector(fSelector);
		// Apply filters dynamically
		addStartDateEndDateFitler(qs, "AUDIT_LOGS__LOG_TIMESTAMP", startDate, endDate);
		addFilter(qs, "AUDIT_LOGS__USER_ID", "==", userId);
		addFilter(qs, "AUDIT_LOGS__PROJECT_ID", "==", projectId);
		addFilter(qs, "AUDIT_LOGS__ENGINE_ID", "==", engineId);
		addRoomIdFilter(qs, roomId);
		addFilter(qs, "AUDIT_LOGS__SESSION_ID", "==", sessionId);
		addMultiValueFilter(qs, "AUDIT_LOGS__METHOD_NAME", methodNames);
		addMultiValueFilter(qs, "AUDIT_LOGS__ENGINE_TYPE", engineTypes);
		addGlobalSearchFilter(queryUtil, qs, searchTerm);

		return QueryExecutionUtility.flushToLong(auditLogsDb, qs);
	}

	/**
	 * Allowlist mapping a front-end filter name to the audit log column(s) whose
	 * distinct values populate that filter's dropdown. The first column is the
	 * human-readable value and is also the one matched by the type-ahead search.
	 *
	 * Acts as the injection guard for {@link #getAuditLogFilterOptionList} - the
	 * caller's filterName never reaches the query directly, only a column resolved
	 * here does. CLOB columns (e.g. REQUEST/RESPONSE) are intentionally excluded:
	 * distinct/search on a CLOB is neither portable across databases nor indexable.
	 *
	 * @param filterName the requested filter
	 * @return the physical column(s) backing that filter
	 * @throws IllegalArgumentException if the filter is not supported
	 */
	private static String[] resolveFilterOptionColumns(String filterName) {
		switch (filterName == null ? "" : filterName.trim().toLowerCase()) {
		case "methodname":
			return new String[] { "AUDIT_LOGS__METHOD_NAME" };
		case "enginetype":
			return new String[] { "AUDIT_LOGS__ENGINE_TYPE" };
		case "user":
			return new String[] { "AUDIT_LOGS__USER_NAME", "AUDIT_LOGS__USER_ID", "AUDIT_LOGS__USER_TYPE" };
		case "roomid":
			return new String[] { "AUDIT_LOGS__ROOM_ID" };
		default:
			throw new IllegalArgumentException("Unsupported filterName '" + filterName
					+ "'. Supported values: methodName, engineType, user, roomId");
		}
	}

	/**
	 * Return the distinct values that populate a single audit log report filter
	 * dropdown. Distinct combinations of the filter's column(s) are returned
	 * directly from the audit logs database - a user (or method/engine type) with
	 * no audit log activity simply will not appear.
	 *
	 * @param userId     scope the values to this user's logs (empty for no scoping)
	 * @param projectId  scope the values to this project (empty for no scoping)
	 * @param engineId   scope the values to this engine (empty for no scoping)
	 * @param engineType scope the values to this engine type (empty for no scoping)
	 * @param filterName the dropdown to populate (methodName, engineType, user)
	 * @param search     optional type-ahead term matched against the display column
	 * @param startDate  lower bound on LOG_TIMESTAMP (null for none) - must match
	 *                   the table's date range so the options reflect the visible
	 *                   rows
	 * @param endDate    upper bound on LOG_TIMESTAMP (null for none)
	 * @param limit      max rows (<= 0 for no limit)
	 * @param offset     row offset (<= 0 for none)
	 * @return distinct rows of the filter's column(s)
	 */
	public static List<String[]> getAuditLogFilterOptionList(String userId, String projectId, String engineId,
			String engineType, String filterName, String search, SemossDate startDate, SemossDate endDate, int limit,
			int offset) {
		IRDBMSEngine auditLogsDb = SystemEngineRegistry.getAuditLogsDb();

		String[] columns = resolveFilterOptionColumns(filterName);

		// SelectQueryStruct is distinct by default, so selecting the column(s) yields
		// the unique values for the dropdown without an explicit group by
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String column : columns) {
			qs.addSelector(new QueryColumnSelector(column));
		}

		// type-ahead search on the display column (always the first, non-CLOB column)
		if (search != null && !search.trim().isEmpty()) {
			qs.addExplicitFilter(auditLogsDb.getQueryUtil().getSearchRegexFilter(columns[0], search.trim()));
		}

		// scope to the same date range as the table so options match the visible rows
		addStartDateEndDateFitler(qs, "AUDIT_LOGS__LOG_TIMESTAMP", startDate, endDate);
		addFilter(qs, "AUDIT_LOGS__USER_ID", "==", userId);
		addFilter(qs, "AUDIT_LOGS__PROJECT_ID", "==", projectId);
		addFilter(qs, "AUDIT_LOGS__ENGINE_ID", "==", engineId);
		addFilter(qs, "AUDIT_LOGS__ENGINE_TYPE", "==", engineType);

		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}

		return QueryExecutionUtility.flushRsToListOfStrArray(auditLogsDb, qs);
	}
}
