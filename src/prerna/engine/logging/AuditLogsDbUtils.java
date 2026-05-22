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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.logging.LogActivityRecord;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.PartitionManager;

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
	 * @param dbSchema
	 * @throws SQLException
	 */
	private static void executeInitDatabaseSchema(IRDBMSEngine engine, Connection conn,
			List<Pair<String, List<Pair<String, String>>>> dbSchema) throws SQLException {

		IRDBMSEngine auditLogsDb = SystemEngineRegistry.getAuditLogsDb();

		String database = engine.getDatabase();
		String schema = engine.getSchema();

		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();

		boolean partitioningSupported = queryUtil.supportsPartitioning()
				&& AbstractSqlQueryUtil.isDatabasePartitioningEnabled();

		String auditLogColDefs = null;

		for (Pair<String, List<Pair<String, String>>> tableSchema : dbSchema) {
			String tableName = tableSchema.getValue0();

			if (partitioningSupported && "AUDIT_LOGS".equalsIgnoreCase(tableName)) {
				// build Audit_Logs table columns definition
				auditLogColDefs = buildColumnDefinitions(tableSchema.getValue1());
				classLogger.info("Partitioning enabled. AUDIT_LOGS creation will be handled separately.");
				continue;
			}
			// table creation or update
			createOrUpdateTable(conn, queryUtil, engine, database, schema, tableSchema, allowIfExistsTable);
		}
		// create partition for Audit_Logs table
		if (partitioningSupported && auditLogColDefs != null) {
			ensureAuditLogsPartitioning(conn, queryUtil, engine, database, schema, auditLogColDefs);
		}
		// create Indexes for Audit_Logs table
		createAuditLogsIndexes(conn, queryUtil, auditLogsDb, database, schema, allowIfExistsIndexs);
	}

	/**
	 * 
	 * @param conn
	 * @param queryUtil
	 * @param engine
	 * @param database
	 * @param schema
	 * @param tableSchema
	 * @param allowIfExistsTable
	 * @return
	 * @throws SQLException
	 */
	private static void createOrUpdateTable(Connection conn, AbstractSqlQueryUtil queryUtil, IRDBMSEngine engine,
			String database, String schema, Pair<String, List<Pair<String, String>>> tableSchema,
			boolean allowIfExistsTable) throws SQLException {

		String tableName = tableSchema.getValue0();
		String[] colNames = tableSchema.getValue1().stream().map(Pair::getValue0).toArray(String[]::new);
		String[] types = tableSchema.getValue1().stream().map(Pair::getValue1).toArray(String[]::new);

		if (allowIfExistsTable) {
			String sql = queryUtil.createTableIfNotExists(tableName, colNames, types);
			executeSql(conn, sql);
		} else {
			if (!queryUtil.tableExists(engine, tableName, database, schema)) {
				String sql = queryUtil.createTable(tableName, colNames, types);
				executeSql(conn, sql);
			}
		}

		List<String> allCols = queryUtil.getTableColumns(conn, tableName, database, schema);
		if (allCols == null) {
			allCols = List.of();
		}

		Set<String> allColsLower = new HashSet<>();
		for (String c : allCols) {
			if (c != null) {
				allColsLower.add(c.toLowerCase(Locale.ROOT));
			}
		}

		for (int i = 0; i < colNames.length; i++) {
			String col = colNames[i];
			if (col == null) {
				continue;
			}

			if (!allColsLower.contains(col.toLowerCase(Locale.ROOT))) {
				String addColumnSql = queryUtil.alterTableAddColumn(tableName, col, types[i]);
				try {
					executeSql(conn, addColumnSql);
				} catch (SQLException e) {
					classLogger.warn("Failed to add column {} to table {}: {}", col, tableName, e.getMessage());
				}
			}
		}
	}

	private static String buildColumnDefinitions(List<Pair<String, String>> cols) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < cols.size(); i++) {
			Pair<String, String> col = cols.get(i);
			sb.append(col.getValue0()).append(" ").append(col.getValue1());
			if (i < cols.size() - 1) {
				sb.append(", ");
			}
		}
		return sb.toString();
	}

	/**
	 * 
	 * @param conn
	 * @param queryUtil
	 * @param engine
	 * @param database
	 * @param schema
	 * @param auditLogColDefs
	 * @return
	 * @throws SQLException
	 */
	private static void ensureAuditLogsPartitioning(Connection conn, AbstractSqlQueryUtil queryUtil,
			IRDBMSEngine engine, String database, String schema, String auditLogColDefs) {
		try {
			boolean exists = queryUtil.tableExists(engine, "AUDIT_LOGS", database, schema);

			PartitionManager.ensurePartitioned(exists, conn, queryUtil, "AUDIT_LOGS", "LOG_TIMESTAMP", auditLogColDefs,
					AbstractSqlQueryUtil.PartitionFrequency.MONTHLY, 2);
		} catch (Exception e) {
			classLogger.warn("Partitioning failed for AUDIT_LOGS: {}", e.getMessage(), e);
		}
	}

	/**
	 * 
	 * @param conn
	 * @param queryUtil
	 * @param auditLogsDb
	 * @param database
	 * @param schema
	 * @param allowIfExistsIndexs
	 * @return
	 * @throws SQLException
	 */
	private static void createAuditLogsIndexes(Connection conn, AbstractSqlQueryUtil queryUtil,
			IRDBMSEngine auditLogsDb, String database, String schema, boolean allowIfExistsIndexs) throws SQLException {

		if (allowIfExistsIndexs) {
			String sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__REQUEST_ID_INDEX", "AUDIT_LOGS", "REQUEST_ID");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__PROJECT_TS_INDEX", "AUDIT_LOGS",
					List.of("PROJECT_ID", "LOG_TIMESTAMP"));
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__USER_TS_INDEX", "AUDIT_LOGS",
					List.of("USER_ID", "LOG_TIMESTAMP"));
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__ENGINE_TS_INDEX", "AUDIT_LOGS",
					List.of("ENGINE_ID", "LOG_TIMESTAMP"));
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__SESSION_ID_INDEX", "AUDIT_LOGS", "SESSION_ID");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AUDIT_LOGS__ROOM_ID_INDEX", "AUDIT_LOGS", "ROOM_ID");
			executeSql(conn, sql);

		} else {

			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__REQUEST_ID_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__REQUEST_ID_INDEX", "AUDIT_LOGS", "REQUEST_ID");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__PROJECT_TS_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__PROJECT_TS_INDEX", "AUDIT_LOGS",
						List.of("PROJECT_ID", "LOG_TIMESTAMP"));
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__USER_TS_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__USER_TS_INDEX", "AUDIT_LOGS",
						List.of("USER_ID", "LOG_TIMESTAMP"));
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__ENGINE_TS_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__ENGINE_TS_INDEX", "AUDIT_LOGS",
						List.of("ENGINE_ID", "LOG_TIMESTAMP"));
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__SESSION_ID_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__SESSION_ID_INDEX", "AUDIT_LOGS", "SESSION_ID");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(auditLogsDb, "AUDIT_LOGS__ROOM_ID_INDEX", "AUDIT_LOGS", database, schema)) {
				String sql = queryUtil.createIndex("AUDIT_LOGS__ROOM_ID_INDEX", "AUDIT_LOGS", "ROOM_ID");
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
			SemossDate startDate, SemossDate endDate, String roomId, String sessionId, int limit, int offset)
			throws SQLException {
		IRDBMSEngine auditLogsDb = SystemEngineRegistry.getAuditLogsDb();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__REQUEST_ID"));
		qs.addSelector(new QueryColumnSelector("MIN_MAX_DURATION__START_TIME"));
		qs.addSelector(new QueryColumnSelector("MIN_MAX_DURATION__END_TIME"));
		qs.addSelector(new QueryColumnSelector("MIN_MAX_DURATION__DURATION"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__ENGINE_NAME"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__ENGINE_TYPE"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__METHOD_NAME"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__REQUEST"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__RESPONSE"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__NUMBER_OF_TOKENS_IN_PROMPT"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__NUMBER_OF_TOKENS_IN_RESPONSE"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__IS_SUCCESS"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__USER_ID"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__SESSION_ID"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__SPAN_ID"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__LOG_TIMESTAMP"));

		// add filters dynamically if present
		addStartDateEndDateFitler(qs, "AUDIT_LOGS__LOG_TIMESTAMP", startDate, endDate);
		addFilter(qs, "AUDIT_LOGS__USER_ID", "==", userId);
		addFilter(qs, "AUDIT_LOGS__PROJECT_ID", "==", projectId);
		addFilter(qs, "AUDIT_LOGS__ENGINE_ID", "==", engineId);
		addFilter(qs, "AUDIT_LOGS__ROOM_ID", "==", roomId);
		addFilter(qs, "AUDIT_LOGS__SESSION_ID", "==", sessionId);
		qs.addOrderBy("AUDIT_LOGS__LOG_TIMESTAMP", "desc");

		// pagination
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}

		SelectQueryStruct minMaxDuration = new SelectQueryStruct();
		minMaxDuration.addSelector(new QueryColumnSelector("AUDIT_LOGS__REQUEST_ID", "REQUEST_ID"));
		minMaxDuration.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
				"AUDIT_LOGS__REQUEST_START_TIME", "START_TIME"));
		minMaxDuration.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX,
				"AUDIT_LOGS__RESPONSE_END_TIME", "END_TIME"));
		minMaxDuration.addSelector(QueryFunctionSelector.makeDateDiffFunctionSelector(QueryFunctionHelper.SECOND,
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, "AUDIT_LOGS__REQUEST_START_TIME",
						"START_TIME"),
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, "AUDIT_LOGS__RESPONSE_END_TIME",
						"END_TIME"),
				"DURATION"));
		// filter for minMaxDuration
		addStartDateEndDateFitler(minMaxDuration, "AUDIT_LOGS__LOG_TIMESTAMP", startDate, endDate);
		addFilter(minMaxDuration, "AUDIT_LOGS__USER_ID", "==", userId);
		addFilter(minMaxDuration, "AUDIT_LOGS__PROJECT_ID", "==", projectId);
		addFilter(minMaxDuration, "AUDIT_LOGS__ENGINE_ID", "==", engineId);
		addFilter(minMaxDuration, "AUDIT_LOGS__ROOM_ID", "==", roomId);
		addFilter(minMaxDuration, "AUDIT_LOGS__SESSION_ID", "==", sessionId);

		minMaxDuration.addGroupBy(new QueryColumnSelector("AUDIT_LOGS__REQUEST_ID"));
		IRelation subQuery = new SubqueryRelationship(minMaxDuration, "MIN_MAX_DURATION", "inner.join",
				new String[] { "AUDIT_LOGS__REQUEST_ID", "MIN_MAX_DURATION__REQUEST_ID", "=" });
		qs.addRelation(subQuery);

		List<LogActivityRecord> activityList = new ArrayList<>();
		List<Map<String, Object>> list = QueryExecutionUtility.flushRsToMap(auditLogsDb, qs);
		for (Map<String, Object> map : list) {
			String requestId = getOrDefault(map.get("REQUEST_ID"), "");
			Timestamp startTime = extractTimestamp(map.get("START_TIME"));
			Timestamp endTime = extractTimestamp(map.get("END_TIME"));
			String request = getOrDefault(map.get("REQUEST"), "");
			String response = getOrDefault(map.get("RESPONSE"), "");
			String engineName = getOrDefault(map.get("ENGINE_NAME"), null);
			String engineType = getOrDefault(map.get("ENGINE_TYPE"), null);
			boolean status = map.get("IS_SUCCESS") instanceof Boolean && (Boolean) map.get("IS_SUCCESS");
			long latency = map.get("DURATION") instanceof Long ? (Long) map.get("DURATION") : 0L;
			int tokens = getIntValue(map.get("NUMBER_OF_TOKENS_IN_PROMPT"))
					+ getIntValue(map.get("NUMBER_OF_TOKENS_IN_RESPONSE"));
			String methodName = getOrDefault(map.get("METHOD_NAME"), "");
			String userIdFromRow = getOrDefault(map.get("USER_ID"), null);
			String sessionIdFromRow = getOrDefault(map.get("SESSION_ID"), null);
			String spanIdFromRow = getOrDefault(map.get("SPAN_ID"), null);
			Timestamp logTimestamp = extractTimestamp(map.get("LOG_TIMESTAMP"));

			activityList.add(new LogActivityRecord(requestId, startTime, endTime, request, response, tokens, latency,
					status, engineName, engineType, methodName, userIdFromRow, sessionIdFromRow, spanIdFromRow,
					logTimestamp));

		}
		return activityList;
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
			SemossDate endDate, String roomId, String sessionId) {
		IRDBMSEngine auditLogsDb = SystemEngineRegistry.getAuditLogsDb();
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

		return QueryExecutionUtility.flushToLong(auditLogsDb, qs);
	}

}
