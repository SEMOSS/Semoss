package prerna.engine.logging;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.logging.LogActivityDto;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class AuditLogsDbUtils {

	private static Logger classLogger = LogManager.getLogger();

	static IRDBMSEngine auditLogsDb;
	static boolean initialized = false;

	private AuditLogsDbUtils() {

	}

	public static void loadAuditLogsDatabase() throws Exception {
		auditLogsDb = (IRDBMSEngine) Utility.getDatabase(Constants.AUDIT_LOGS_DB);
		initEngineAsAuditDatabase(auditLogsDb);
		initialized = true;
	}

	/**
	 * @param engine
	 * @param conn
	 * @param columnNamesAndTypes
	 * @throws SQLException
	 */
	private static void executeInitDatabaseSchema(IRDBMSEngine engine, Connection conn,
			List<Pair<String, List<Pair<String, String>>>> dbSchema) throws SQLException {

		String database = engine.getDatabase();
		String schema = engine.getSchema();

		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
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
				if (!queryUtil.tableExists(engine, tableName, database, schema)) {
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
			// reset the local master metadata for model engine if we remade the OWL
			Utility.synchronizeEngineMetadata(auditLogsDb.getEngineId());
		}

		Connection conn = null;
		try {
			conn = auditLogsDb.makeConnection();
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
	 * @param date
	 * @param roomId
	 * @param sessionId
	 * @return
	 * @throws SQLException
	 */
	public static List<LogActivityDto> getAuditLogsTimeLineDatas(String userId, String projectId, String engineId,
			String date, String roomId, String sessionId) throws SQLException {

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__REQUEST_START_TIME"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__ENGINE_NAME"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__ENGINE_TYPE"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__RESPONSE_END_TIME"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__REQUEST"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__RESPONSE"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__NUMBER_OF_TOKENS_IN_PROMPT"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__NUMBER_OF_TOKENS_IN_RESPONSE"));
		qs.addSelector(new QueryColumnSelector("AUDIT_LOGS__IS_SUCCESS"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("AUDIT_LOGS__LOG_TIMESTAMP", "<=", date));
		OrQueryFilter or = new OrQueryFilter();
		AndQueryFilter and = new AndQueryFilter();
		or.addFilter(SimpleQueryFilter.makeColToValFilter("AUDIT_LOGS__USER_ID", "==", userId));
		or.addFilter(SimpleQueryFilter.makeColToValFilter("AUDIT_LOGS__PROJECT_ID", "==", projectId));
		or.addFilter(SimpleQueryFilter.makeColToValFilter("AUDIT_LOGS__ENGINE_ID", "==", engineId));
		or.addFilter(SimpleQueryFilter.makeColToValFilter("AUDIT_LOGS__ROOM_ID", "==", roomId));
		or.addFilter(SimpleQueryFilter.makeColToValFilter("AUDIT_LOGS__SESSION_ID", "==", sessionId));
		and.addFilter(or);
		qs.addExplicitFilter(and);
		qs.addOrderBy("AUDIT_LOGS__RESPONSE_END_TIME", "desc");

		List<LogActivityDto> activityList = new ArrayList<>();
		List<Map<String, Object>> list = QueryExecutionUtility.flushRsToMap(auditLogsDb, qs);
		list.forEach(map -> {
			Timestamp startTime = null;
			Timestamp endTime = null;
			Timestamp startTimeMS = null;
			Timestamp endTimeMS = null;
			String payload = null;
			String response = null;
			int tokens = 0;
			Boolean status = true;
			long latency = 0L;
			String engineName = null;
			String engineType = null;

			if (map.get("REQUEST_START_TIME") != null && !map.get("REQUEST_START_TIME").equals("")
					&& map.get("RESPONSE_END_TIME") != null && !map.get("RESPONSE_END_TIME").equals("")) {
				if (map.get("REQUEST_START_TIME") != null && !map.get("REQUEST_START_TIME").equals("")) {
					startTimeMS = Utility.getSqlTimestampUTC((SemossDate) map.get("REQUEST_START_TIME"));
					LocalDateTime truncated = startTimeMS.toLocalDateTime().truncatedTo(ChronoUnit.SECONDS);
					startTime = Timestamp.valueOf(truncated);
				}
				if (map.get("ENGINE_NAME") != null && !map.get("ENGINE_NAME").equals("")) {
					engineName = (String) map.get("ENGINE_NAME");
				}
				if (map.get("ENGINE_TYPE") != null && !map.get("ENGINE_TYPE").equals("")) {
					engineType = (String) map.get("ENGINE_TYPE");
				}
				if (map.get("RESPONSE_END_TIME") != null && !map.get("RESPONSE_END_TIME").equals("")) {
					endTimeMS = Utility.getSqlTimestampUTC((SemossDate) map.get("RESPONSE_END_TIME"));
					LocalDateTime truncated = endTimeMS.toLocalDateTime().truncatedTo(ChronoUnit.SECONDS);
					endTime = Timestamp.valueOf(truncated);
				}
				if (map.get("REQUEST") != null && !map.get("REQUEST").equals("")) {
					payload = (String) map.get("REQUEST");
				}
				if (map.get("RESPONSE") != null && !map.get("RESPONSE").equals("")) {
					response = (String) map.get("RESPONSE");
				}
				if (map.get("IS_SUCCESS") != null && Boolean.valueOf((boolean) map.get("IS_SUCCESS"))) {
					status = Boolean.valueOf((boolean) map.get("IS_SUCCESS"));
				}
				if (map.get("NUMBER_OF_TOKENS_IN_PROMPT") != null
						&& !map.get("NUMBER_OF_TOKENS_IN_PROMPT").equals("")) {
					tokens = (Integer) map.get("NUMBER_OF_TOKENS_IN_PROMPT");
				}
				if (map.get("REQUEST_START_TIME") != null && !map.get("REQUEST_START_TIME").equals("")
						&& map.get("RESPONSE_END_TIME") != null && !map.get("RESPONSE_END_TIME").equals("")) {
					latency = extractMilliseconds(startTimeMS, endTimeMS);
				}
				activityList.add(new LogActivityDto(startTime, endTime, payload, response, tokens, latency, status,
						engineName, engineType));
			}

		});
		return activityList;
	}

	/**
	 * 
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	private static long extractMilliseconds(Timestamp startTime, Timestamp endTime) {
		LocalTime start = startTime.toLocalDateTime().toLocalTime();
		LocalTime end = endTime.toLocalDateTime().toLocalTime();
		long millis = Duration.between(start, end).toMillis();
		return millis;
	}

}
