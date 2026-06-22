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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.logging.LogActivityRecord;
import prerna.logging.RequestDurationRecord;
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

		addStartDateEndDateFitler(qs, "AUDIT_LOGS__LOG_TIMESTAMP", startDate, endDate);
		addFilter(qs, "AUDIT_LOGS__USER_ID", "==", userId);
		addFilter(qs, "AUDIT_LOGS__PROJECT_ID", "==", projectId);
		addFilter(qs, "AUDIT_LOGS__ENGINE_ID", "==", engineId);
		addFilter(qs, "AUDIT_LOGS__ROOM_ID", "==", roomId);
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

		Set<String> requestIds = new LinkedHashSet<>();
		for (Map<String, Object> row : pageRows) {
			String reqId = getOrDefault(row.get("REQUEST_ID"), "");
			if (!reqId.isEmpty()) {
				requestIds.add(reqId);
			}
		}

		// Step 2: aggregate MIN/MAX/DURATION only for the REQUEST_IDs in this page
		Map<String, RequestDurationRecord> durationsByRequestId = fetchDurationsForRequestIds(auditLogsDb, requestIds);

		List<LogActivityRecord> activityList = new ArrayList<>(pageRows.size());
		for (Map<String, Object> map : pageRows) {
			String requestId = getOrDefault(map.get("REQUEST_ID"), "");
			RequestDurationRecord duration = durationsByRequestId.get(requestId);
			Timestamp startTime = duration != null ? duration.startTime() : null;
			Timestamp endTime = duration != null ? duration.endTime() : null;
			long latency = duration != null ? duration.durationSeconds() : 0L;

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
			Timestamp logTimestamp = extractTimestamp(map.get("LOG_TIMESTAMP"));

			activityList.add(new LogActivityRecord(requestId, startTime, endTime, request, response, tokens, latency,
					status, engineName, engineType, methodName, userNameFromRow, userIdFromRow, sessionIdFromRow,
					spanIdFromRow, logTimestamp));

		}
		return activityList;
	}

	/**
	 * 
	 * @param auditLogsDb
	 * @param requestIds
	 * @return
	 */
	private static Map<String, RequestDurationRecord> fetchDurationsForRequestIds(IRDBMSEngine auditLogsDb,
			Set<String> requestIds) {
		if (requestIds.isEmpty()) {
			return Collections.emptyMap();
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__REQUEST_ID", "REQUEST_ID"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
				"AUDIT_LOGS__REQUEST_START_TIME", "START_TIME"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX,
				"AUDIT_LOGS__RESPONSE_END_TIME", "END_TIME"));
		qs.addSelector(QueryFunctionSelector.makeDateDiffFunctionSelector(QueryFunctionHelper.SECOND,
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, "AUDIT_LOGS__REQUEST_START_TIME",
						"START_TIME"),
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, "AUDIT_LOGS__RESPONSE_END_TIME",
						"END_TIME"),
				"DURATION"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("AUDIT_LOGS__REQUEST_ID", "==", new ArrayList<>(requestIds)));
		qs.addGroupBy(new QueryColumnSelector("AUDIT_LOGS__REQUEST_ID"));

		Map<String, RequestDurationRecord> result = new HashMap<>(requestIds.size());
		for (Map<String, Object> row : QueryExecutionUtility.flushRsToMap(auditLogsDb, qs)) {
			String reqId = getOrDefault(row.get("REQUEST_ID"), "");
			if (reqId.isEmpty()) {
				continue;
			}
			Timestamp startTime = extractTimestamp(row.get("START_TIME"));
			Timestamp endTime = extractTimestamp(row.get("END_TIME"));
			long durationSeconds = row.get("DURATION") instanceof Long ? (Long) row.get("DURATION") : 0L;
			result.put(reqId, new RequestDurationRecord(startTime, endTime, durationSeconds));
		}
		return result;
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
		// NOTE: REQUEST and RESPONSE are CLOB columns and are intentionally excluded -
		// searching/filtering on CLOB columns is not portable across databases and
		// cannot use an index.
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(queryUtil.getSearchRegexFilter("AUDIT_LOGS__METHOD_NAME", value));
		orFilter.addFilter(queryUtil.getSearchRegexFilter("AUDIT_LOGS__ENGINE_TYPE", value));

		qs.addExplicitFilter(orFilter);
	}

	/**
	 * 
	 * @param dateObj
	 * @return
	 */
	private static Timestamp extractTimestamp(Object dateObj) {
		if (dateObj instanceof SemossDate) {
			Timestamp ts = Utility.getSqlTimestampUTC((SemossDate) dateObj);
			return Timestamp.valueOf(ts.toLocalDateTime().truncatedTo(ChronoUnit.SECONDS));
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
		addFilter(qs, "AUDIT_LOGS__ROOM_ID", "==", roomId);
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
		default:
			throw new IllegalArgumentException(
					"Unsupported filterName '" + filterName + "'. Supported values: methodName, engineType, user");
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
	 * @param limit      max rows (<= 0 for no limit)
	 * @param offset     row offset (<= 0 for none)
	 * @return distinct rows of the filter's column(s)
	 */
	public static List<String[]> getAuditLogFilterOptionList(String userId, String projectId, String engineId,
			String engineType, String filterName, String search, int limit, int offset) {
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
