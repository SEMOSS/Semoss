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
package prerna.engine.impl.model.inferencetracking;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.json.JSONArray;
import org.json.JSONObject;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.model.MessageFeedback;
import prerna.engine.impl.model.ModelUsageRestrictionUtility;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.MessageType;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.query.querystruct.selectors.QueryTypedColumnSelector;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class ModelInferenceLogsUtils {

	private static Logger classLogger = LogManager.getLogger(ModelInferenceLogsUtils.class);

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	public static final String WORKSPACE_PROJECT_TAG = "Workspace_Project";

	// Constants for Table
	private static final String MESSAGE_TABLE_NAME = "MESSAGE__";
	private static final String AGENT_TABLE_NAME = "AGENT__";
	private static final String ROOM_TABLE_NAME = "ROOM__";
	private static final String FEEDBACK_TABLE_NAME = "FEEDBACK__";
	static boolean initialized = false;

	/**
	 * Initializes and migrates the model-inference logging database schema.
	 *
	 * @throws Exception if schema bootstrap or migration fails
	 */
	public static void initModelInferenceLogsDatabase() throws Exception {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		ModelInferenceLogsOwlCreator modelInfCreator = new ModelInferenceLogsOwlCreator(
				modelInferenceLogsDb.getQueryUtil());
		if (modelInfCreator.needsRemake(modelInferenceLogsDb)) {
			modelInfCreator.remakeOwl(modelInferenceLogsDb);
		}

		Connection conn = null;
		try {
			conn = modelInferenceLogsDb.getConnection();
			executeInitModelInferenceDatabase(modelInferenceLogsDb, conn, modelInfCreator.getDBSchema());
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, conn, null, null);
		}
	}

	/**
	 * Creates missing tables/columns and applies index/migration updates for the
	 * model-inference logging schema.
	 *
	 * @param engine   model-inference logs engine
	 * @param conn     database connection
	 * @param dbSchema table and column metadata to apply
	 * @throws SQLException if DDL execution fails
	 */
	private static void executeInitModelInferenceDatabase(IRDBMSEngine engine, Connection conn,
			List<Pair<String, List<Pair<String, String>>>> dbSchema) throws SQLException {

		String database = engine.getDatabase();
		String schema = engine.getSchema();

		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();

		boolean roomIdColumnWasAdded = false;
		boolean modelIdColumnWasAdded = false;

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

					// was room id just added? 2025-06-26 addition. if so update w/ insight id
					if (tableName.equalsIgnoreCase("ROOM") && col.equalsIgnoreCase("ROOM_ID")) {
						roomIdColumnWasAdded = true;
					}
					if (tableName.equalsIgnoreCase("MESSAGE") && col.equalsIgnoreCase("ROOM_ID")) {
						roomIdColumnWasAdded = true;
					}

					// was model id just added? 2025-06-26 addition. if so update w/ agent id
					if (tableName.equalsIgnoreCase("ROOM") && col.equalsIgnoreCase("MODEL_ID")) {
						modelIdColumnWasAdded = true;
					}
					if (tableName.equalsIgnoreCase("MESSAGE") && col.equalsIgnoreCase("MODEL_ID")) {
						modelIdColumnWasAdded = true;
					}
				}
			}
		}

		// was roomId just added
		if (roomIdColumnWasAdded) {
			dropRoomMessageConstraints(conn);
			migrateRoomAndMessageIds(conn);
		}

		// was modelId just added
		if (modelIdColumnWasAdded) {
			migrateAgentAndModelIds(conn);
		}

		if (allowIfExistsIndexs) {
			String sql = queryUtil.createIndexIfNotExists("MESSAGE_INSIGHT_ID_INDEX", "MESSAGE", "INSIGHT_ID");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("MESSAGE_ROOM_ID_INDEX", "MESSAGE", "ROOM_ID");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("MESSAGE_USER_ID_INDEX", "MESSAGE", "USER_ID");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("MESSAGE_DATE_CREATED_INDEX", "MESSAGE", "DATE_CREATED");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("ROOM_INSIGHT_ID_INDEX", "ROOM", "INSIGHT_ID");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("ROOM_ROOM_ID_INDEX", "ROOM", "ROOM_ID");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("ROOM_USER_ID_INDEX", "ROOM", "USER_ID");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("ROOM_IS_ACTIVE_INDEX", "ROOM", "IS_ACTIVE");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("ROOM_WORKSPACE_ID_INDEX", "ROOM", "WORKSPACE_ID");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("WORKSPACE_OWNER_INDEX", "WORKSPACE", "OWNER");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AGENT_RUN_RUN_ID_INDEX", "AGENT_RUN", "RUN_ID");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AGENT_RUN_ROOM_ID_INDEX", "AGENT_RUN", "ROOM_ID");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AGENT_RUN_PARENT_RUN_ID_INDEX", "AGENT_RUN", "PARENT_RUN_ID");
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AGENT_RUN_USER_WORKSPACE_DATE_INDEX", "AGENT_RUN",
					Arrays.asList("USER_ID", "WORKSPACE_ID", "DATE_CREATED"));
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AGENT_RUN_USER_ROOM_DATE_INDEX", "AGENT_RUN",
					Arrays.asList("USER_ID", "ROOM_ID", "DATE_CREATED"));
			executeSql(conn, sql);

			sql = queryUtil.createIndexIfNotExists("AGENT_RUN_ACTION_RUN_ID_INDEX", "AGENT_RUN_ACTION", "RUN_ID");
			executeSql(conn, sql);
		} else {
			if (!queryUtil.indexExists(engine, "MESSAGE_INSIGHT_ID_INDEX", "MESSAGE", database, schema)) {
				String sql = queryUtil.createIndex("MESSAGE_INSIGHT_ID_INDEX", "MESSAGE", "INSIGHT_ID");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "MESSAGE_ROOM_ID_INDEX", "MESSAGE", database, schema)) {
				String sql = queryUtil.createIndex("MESSAGE_ROOM_ID_INDEX", "MESSAGE", "ROOM_ID");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "MESSAGE_USER_ID_INDEX", "MESSAGE", database, schema)) {
				String sql = queryUtil.createIndex("MESSAGE_USER_ID_INDEX", "MESSAGE", "USER_ID");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "MESSAGE_DATE_CREATED_INDEX", "MESSAGE", database, schema)) {
				String sql = queryUtil.createIndex("MESSAGE_DATE_CREATED_INDEX", "MESSAGE", "DATE_CREATED");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "ROOM_INSIGHT_ID_INDEX", "ROOM", database, schema)) {
				String sql = queryUtil.createIndex("ROOM_INSIGHT_ID_INDEX", "ROOM", "INSIGHT_ID");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "ROOM_ROOM_ID_INDEX", "ROOM", database, schema)) {
				String sql = queryUtil.createIndex("ROOM_ROOM_ID_INDEX", "ROOM", "ROOM_ID");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "ROOM_USER_ID_INDEX", "ROOM", database, schema)) {
				String sql = queryUtil.createIndex("ROOM_USER_ID_INDEX", "ROOM", "USER_ID");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "ROOM_IS_ACTIVE_INDEX", "ROOM", database, schema)) {
				String sql = queryUtil.createIndex("ROOM_IS_ACTIVE_INDEX", "ROOM", "IS_ACTIVE");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "WORKSPACE_OWNER_INDEX", "WORKSPACE", database, schema)) {
				String sql = queryUtil.createIndex("WORKSPACE_OWNER_INDEX", "WORKSPACE", "OWNER");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "AGENT_RUN_RUN_ID_INDEX", "AGENT_RUN", database, schema)) {
				String sql = queryUtil.createIndex("AGENT_RUN_RUN_ID_INDEX", "AGENT_RUN", "RUN_ID");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "AGENT_RUN_ROOM_ID_INDEX", "AGENT_RUN", database, schema)) {
				String sql = queryUtil.createIndex("AGENT_RUN_ROOM_ID_INDEX", "AGENT_RUN", "ROOM_ID");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "AGENT_RUN_PARENT_RUN_ID_INDEX", "AGENT_RUN", database, schema)) {
				String sql = queryUtil.createIndex("AGENT_RUN_PARENT_RUN_ID_INDEX", "AGENT_RUN", "PARENT_RUN_ID");
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "AGENT_RUN_USER_WORKSPACE_DATE_INDEX", "AGENT_RUN", database, schema)) {
				String sql = queryUtil.createIndex("AGENT_RUN_USER_WORKSPACE_DATE_INDEX", "AGENT_RUN",
						Arrays.asList("USER_ID", "WORKSPACE_ID", "DATE_CREATED"));
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "AGENT_RUN_USER_ROOM_DATE_INDEX", "AGENT_RUN", database, schema)) {
				String sql = queryUtil.createIndex("AGENT_RUN_USER_ROOM_DATE_INDEX", "AGENT_RUN",
						Arrays.asList("USER_ID", "ROOM_ID", "DATE_CREATED"));
				executeSql(conn, sql);
			}

			if (!queryUtil.indexExists(engine, "AGENT_RUN_ACTION_RUN_ID_INDEX", "AGENT_RUN_ACTION", database, schema)) {
				String sql = queryUtil.createIndex("AGENT_RUN_ACTION_RUN_ID_INDEX", "AGENT_RUN_ACTION", "RUN_ID");
				executeSql(conn, sql);
			}
		}
	}

	/**
	 * Backfills {@code ROOM_ID} values from legacy {@code INSIGHT_ID} values.
	 *
	 * @param conn database connection
	 */
	private static void migrateRoomAndMessageIds(Connection conn) {
		try (Statement stmt = conn.createStatement()) {
			int rCount = stmt
					.executeUpdate("UPDATE ROOM SET ROOM_ID = INSIGHT_ID WHERE ROOM_ID IS NULL OR ROOM_ID = ''");
			int mCount = stmt
					.executeUpdate("UPDATE MESSAGE SET ROOM_ID = INSIGHT_ID WHERE ROOM_ID IS NULL OR ROOM_ID = ''");
			classLogger.info("Room/Message room_id migration updated {} ROOM rows and {} MESSAGE rows.", rCount,
					mCount);
		} catch (SQLException ex) {
			classLogger.error("Failed to migrate legacy ROOM_ID fields", ex);
		}

	}

	/**
	 * Backfills {@code MODEL_ID} values from legacy {@code AGENT_ID} values.
	 *
	 * @param conn database connection
	 */
	private static void migrateAgentAndModelIds(Connection conn) {
		try (Statement stmt = conn.createStatement()) {
			int rCount = stmt
					.executeUpdate("UPDATE ROOM SET MODEL_ID = AGENT_ID WHERE MODEL_ID IS NULL OR MODEL_ID = ''");
			int mCount = stmt
					.executeUpdate("UPDATE MESSAGE SET MODEL_ID = AGENT_ID WHERE MODEL_ID IS NULL OR MODEL_ID = ''");
			classLogger.info("Room/Message model_id migration updated {} ROOM rows and {} MESSAGE rows.", rCount,
					mCount);
		} catch (SQLException ex) {
			classLogger.error("Failed to migrate legacy AGENT_ID fields", ex);
		}
	}

	/**
	 * Drops legacy ROOM/MESSAGE constraints when ROOM_ID migration requires a
	 * constraint reset.
	 *
	 * @param conn database connection
	 */
	private static void dropRoomMessageConstraints(Connection conn) {
		String dropMessageFK = "ALTER TABLE MESSAGE DROP CONSTRAINT MESSAGE_INSIGHT_ID_ROOM_INSIGHT_ID_KEY";
		String dropRoomPK = "ALTER TABLE ROOM DROP CONSTRAINT ROOM_KEY";
		try {
			executeSql(conn, dropMessageFK);
		} catch (SQLException ex) {
			classLogger.warn("Tried to drop MESSAGE_INSIGHT_ID_ROOM_INSIGHT_ID_KEY but it probably does not exist", ex);
		}
		try {
			executeSql(conn, dropRoomPK);
		} catch (SQLException ex) {
			classLogger.warn("Tried to drop ROOM_KEY but it probably does not exist", ex);
		}
	}

	/**
	 * Executes a single SQL statement.
	 *
	 * @param conn database connection
	 * @param sql  SQL statement to execute
	 * @throws SQLException if execution fails
	 */
	private static void executeSql(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			classLogger.info("Running sql {}", sql);
			stmt.execute(sql);
		}
	}

	/**
	 * Checks whether the given user authored the given message.
	 *
	 * @param userId    user identifier
	 * @param messageId message identifier
	 * @return {@code true} when the message belongs to the user
	 */
	public static boolean userIsMessageAuthor(String userId, String messageId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT, "MESSAGE__MESSAGE_ID", "Counts"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_ID", "==", messageId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__USER_ID", "==", userId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs)) {
			while (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val == null) {
					return false;
				}
				int intVal = ((Number) val).intValue();
				if (intVal > 0) {
					return true;
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to verify message ownership for userId '{}' and messageId '{}'.", userId,
					messageId, e);
		}
		return false;
	}

	/**
	 * Returns true when the user submitted the given batch (batch_submit row
	 * exists).
	 */
	public static boolean userOwnsBatch(String userId, String providerBatchId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) {
			return false;
		}
		String query = "SELECT COUNT(*) FROM MESSAGE WHERE TRANSACTION_ID = ? AND USER_ID = ? AND MESSAGE_METHOD = 'batch_submit'";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = db.getPreparedStatement(query);
			ps.setString(1, providerBatchId);
			ps.setString(2, userId);
			rs = ps.executeQuery();
			if (rs.next()) {
				return rs.getInt(1) > 0;
			}
		} catch (Exception e) {
			classLogger.warn("Batch ownership check failed for batch '{}': {}", providerBatchId, e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
		return false;
	}

	/**
	 * Returns the user's batch submissions for an engine, most recent first. Each
	 * map has: providerBatchId, submittedAt, engineId, requestCount.
	 */
	public static List<Map<String, Object>> getUserBatches(String userId, String engineId, int limit) {
		List<Map<String, Object>> out = new ArrayList<>();
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) {
			return out;
		}
		String query = "SELECT TRANSACTION_ID, DATE_CREATED, AGENT_ID, MESSAGE_DATA FROM MESSAGE"
				+ " WHERE USER_ID = ? AND AGENT_ID = ? AND MESSAGE_METHOD = 'batch_submit'"
				+ " ORDER BY DATE_CREATED DESC LIMIT ?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = db.getPreparedStatement(query);
			ps.setString(1, userId);
			ps.setString(2, engineId);
			ps.setInt(3, limit);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				row.put("batchId", rs.getString("TRANSACTION_ID"));
				row.put("submittedAt", rs.getString("DATE_CREATED"));
				row.put("engineId", rs.getString("AGENT_ID"));
				String msgData = db.getQueryUtil().handleBlobRetrieval(rs, "MESSAGE_DATA");
				if (msgData != null) {
					try {
						row.put("requestCount", Integer.parseInt(msgData.trim()));
					} catch (NumberFormatException ignore) {
						// non-numeric stored data
					}
				}
				out.add(row);
			}
		} catch (Exception e) {
			classLogger.warn("Failed to list batches for user '{}'", userId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
		return out;
	}

	/**
	 * Returns the stored input prompts for a batch as customId -> command map.
	 * Queries INPUT rows in the batch room (ROOM_ID = "mb_<batchId>") written at
	 * submit time. TRANSACTION_ID has the form "batchId.customId", so customId is
	 * extracted as the suffix after "batchId.".
	 */
	public static Map<String, String> getBatchInputs(String userId, String providerBatchId) {
		Map<String, String> out = new HashMap<>();
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) {
			return out;
		}
		String roomId = "mb_" + providerBatchId;
		String prefix = providerBatchId + ".";
		String query = "SELECT TRANSACTION_ID, MESSAGE_DATA FROM MESSAGE"
				+ " WHERE ROOM_ID = ? AND USER_ID = ? AND MESSAGE_TYPE = 'INPUT' AND MESSAGE_METHOD = 'batch'";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = db.getPreparedStatement(query);
			ps.setString(1, roomId);
			ps.setString(2, userId);
			rs = ps.executeQuery();
			while (rs.next()) {
				String txnId = rs.getString("TRANSACTION_ID");
				String command = db.getQueryUtil().handleBlobRetrieval(rs, "MESSAGE_DATA");
				if (txnId != null && txnId.startsWith(prefix) && command != null) {
					out.put(txnId.substring(prefix.length()), command);
				}
			}
		} catch (Exception e) {
			classLogger.warn("Failed to retrieve batch inputs for batch '{}'", providerBatchId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
		return out;
	}

	/**
	 * Sets MESSAGE_TOKENS + INPUT_TOKENS on the submit-time INPUT row for a batch
	 * item. The INPUT row is written at submit time before token counts are known;
	 * this back-fills them at results time so usage analytics (which derive the
	 * input/ response split from MESSAGE_TOKENS keyed on MESSAGE_TYPE) are correct.
	 * Matched by the per-item TRANSACTION_ID ("batchId.customId").
	 */
	public static void updateBatchInputTokens(String transactionId, Integer inputTokens) {
		if (inputTokens == null) {
			return;
		}
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) {
			return;
		}
		String query = "UPDATE MESSAGE SET MESSAGE_TOKENS = ?, INPUT_TOKENS = ?"
				+ " WHERE TRANSACTION_ID = ? AND MESSAGE_TYPE = 'INPUT' AND MESSAGE_METHOD = 'batch'";
		PreparedStatement ps = null;
		try {
			ps = db.getPreparedStatement(query);
			ps.setInt(1, inputTokens);
			ps.setInt(2, inputTokens);
			ps.setString(3, transactionId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.warn("Failed to update batch input tokens for transaction '{}'", transactionId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	/**
	 * Upserts feedback for a response message.
	 *
	 * @param feedback feedback payload
	 */
	public static void recordFeedback(MessageFeedback feedback) {
		if (feedbackExists(feedback.getMessageId())) {
			updateFeedback(feedback);
		} else {
			insertFeedback(feedback);
		}
	}

	/**
	 * Checks whether a feedback record exists for a response message.
	 *
	 * @param messageId message identifier
	 * @return {@code true} if feedback already exists
	 */
	public static boolean feedbackExists(String messageId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT,
				FEEDBACK_TABLE_NAME + "MESSAGE_ID", "Counts"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(FEEDBACK_TABLE_NAME + "MESSAGE_ID", "==", messageId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs)) {
			while (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val == null) {
					return false;
				}
				int intVal = ((Number) val).intValue();
				if (intVal > 0) {
					return true;
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to check whether feedback exists for messageId '{}'.", messageId, e);
			throw new SemossPixelException("Error while checking feedbackExists or not ." + e.getMessage());
		}
		return false;
	}

	/**
	 * Inserts a new feedback row for a response message.
	 *
	 * @param feedback feedback payload
	 */
	public static void insertFeedback(MessageFeedback feedback) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "INSERT INTO FEEDBACK (MESSAGE_ID, FEEDBACK_TEXT, FEEDBACK_DATE, RATING) "
				+ "VALUES (?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, feedback.getMessageId());
			ps.setString(index++, feedback.getFeedbackText());
			ps.setTimestamp(index++, Timestamp.valueOf(feedback.getFeedbackDate().getLocalDateTime()));
			ps.setBoolean(index++, feedback.getRating());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to insert feedback for messageId '{}'.", feedback.getMessageId(), e);
			throw new SemossPixelException("Unable to insert feedback: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Updates an existing feedback row for a response message.
	 *
	 * @param feedback feedback payload
	 */
	public static void updateFeedback(MessageFeedback feedback) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		try {
			PreparedStatement ps = modelInferenceLogsDb.getPreparedStatement(
					"UPDATE FEEDBACK SET FEEDBACK_TEXT=?, FEEDBACK_DATE=?, RATING=? WHERE MESSAGE_ID=?");
			if (ps == null) {
				throw new IllegalArgumentException("Error generating prepared statement to update feedback");
			}
			try {
				int parameterIndex = 1;
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps, feedback.getFeedbackText(),
						parameterIndex++, GSON);
				ps.setTimestamp(parameterIndex++, Timestamp.valueOf(feedback.getFeedbackDate().getLocalDateTime()));
				ps.setBoolean(parameterIndex++, feedback.getRating());
				ps.setString(parameterIndex++, feedback.getMessageId());
				ps.executeUpdate();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (Exception e) {
				classLogger.error("Failed to update feedback row for messageId '{}'.", feedback.getMessageId(), e);
				throw e;
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, ps);
			}
		} catch (Exception e) {
			classLogger.error("Feedback update flow failed for messageId '{}'.", feedback.getMessageId(), e);
		}
	}

	/** USAGE HELPER FUNCTIONS */

	/**
	 * Returns raw usage rows for a model/engine, including token and request
	 * metadata.
	 *
	 * @param engineId  engine identifier
	 * @param limit     max rows to return (nullable)
	 * @param offset    rows to skip (nullable)
	 * @param startDate inclusive start date filter (nullable)
	 * @param endDate   inclusive end date filter (nullable)
	 * @return usage rows for the requested engine
	 */
	public static List<Map<String, Object>> getOverAllEngineUsageFromModelInferenceLogs(String engineId, String limit,
			String offset, String startDate, String endDate) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_ID"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_TYPE"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_TOKENS"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_METHOD"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "DATE_CREATED"));
		qs.addSelector(new QueryColumnSelector(AGENT_TABLE_NAME + "AGENT_NAME"));
		qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_NAME"));

		qs.addRelation(MESSAGE_TABLE_NAME + "AGENT_ID", AGENT_TABLE_NAME + "AGENT_ID", "left.join");
		qs.addRelation(MESSAGE_TABLE_NAME + "AGENT_ID", ROOM_TABLE_NAME + "AGENT_ID", "left.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__AGENT_ID", "==", engineId));
		addStartDateEndDateFitler(qs, startDate, endDate);

		addLimitAndOffSet(qs, limit, offset);
		// order descending
		qs.addOrderBy(MESSAGE_TABLE_NAME + "DATE_CREATED", "DESC");
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Returns token and request totals by project for a given engine.
	 *
	 * @param engineId  engine identifier
	 * @param limit     max rows to return (nullable)
	 * @param offset    rows to skip (nullable)
	 * @param startDate inclusive start date filter (nullable)
	 * @param endDate   inclusive end date filter (nullable)
	 * @return aggregated per-project usage rows
	 */
	public static List<Map<String, Object>> getTokenUsagePerProjectForEngine(String engineId, String limit,
			String offset, String startDate, String endDate) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_NAME"));

		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM,
				MESSAGE_TABLE_NAME + "MESSAGE_TOKENS", "TOTAL_NUMBER_OF_TOKENS"));

		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT,
				MESSAGE_TABLE_NAME + "MESSAGE_ID", "TOTAL_NUMBER_OF_REQUEST"));

		qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_ID"));
		qs.addRelation(MESSAGE_TABLE_NAME + "AGENT_ID", ROOM_TABLE_NAME + "AGENT_ID", "left.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "AGENT_ID", "==", engineId));
		addStartDateEndDateFitler(qs, startDate, endDate);

		addLimitAndOffSet(qs, limit, offset);
		qs.addGroupBy(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_NAME"));
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Applies pagination values to a query struct.
	 *
	 * @param qs     query struct to update
	 * @param limit  max rows to return (nullable)
	 * @param offset rows to skip (nullable)
	 */
	private static void addLimitAndOffSet(SelectQueryStruct qs, String limit, String offset) {
		Long long_limit = -1L;
		Long long_offset = -1L;
		if (limit != null && !limit.trim().isEmpty()) {
			long_limit = ((Number) Double.parseDouble(limit)).longValue();
		}
		if (offset != null && !offset.trim().isEmpty()) {
			long_offset = ((Number) Double.parseDouble(offset)).longValue();
		}
		qs.setLimit(long_limit);
		qs.setOffSet(long_offset);
	}

	/**
	 * Returns token usage totals grouped by user for a specific engine.
	 *
	 * @param engineId  engine identifier
	 * @param limit     max rows to return (nullable)
	 * @param offset    rows to skip (nullable)
	 * @param startDate inclusive start date filter (nullable)
	 * @param endDate   inclusive end date filter (nullable)
	 * @return per-user usage rows
	 */
	public static List<Map<String, Object>> getUserUsagePerEngine(String engineId, String limit, String offset,
			String startDate, String endDate) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_NAME"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_ID"));

		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM,
				MESSAGE_TABLE_NAME + "MESSAGE_TOKENS", "TOTAL_NUMBER_OF_TOKENS"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "AGENT_ID", "==", engineId));
		addStartDateEndDateFitler(qs, startDate, endDate);

		addLimitAndOffSet(qs, limit, offset);
		qs.addGroupBy(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_NAME"));

		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Adds a DATE_CREATED range filter to a message query when both dates are
	 * provided.
	 *
	 * @param qs        query struct to update
	 * @param startDate inclusive start date (nullable)
	 * @param endDate   inclusive end date (nullable)
	 */
	private static void addStartDateEndDateFitler(SelectQueryStruct qs, String startDate, String endDate) {
		if ((startDate != null && !startDate.trim().isEmpty()) && (endDate != null && !endDate.trim().isEmpty())) {
			AndQueryFilter andFilters = new AndQueryFilter();
			andFilters.addFilter(
					SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "DATE_CREATED", ">=", startDate));
			andFilters.addFilter(
					SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "DATE_CREATED", "<=", endDate));
			qs.addExplicitFilter(andFilters);
		}
	}

	/**
	 * Returns top-level message usage metrics for a project.
	 *
	 * @param projectId project identifier
	 * @return project usage summary
	 */
	public static Map<String, Object> getProjectUsageFromModelInferenceLogs(String projectId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		// First get a list of roomIds from Room
		List<String> roomIdList = getRoomIdListPerProject(projectId);
		// Second query against message to find number of unique calls? Not sure what we
		// are tracking
		// from projects just yet
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT, "MESSAGE__MESSAGE_ID",
				"Unique_Calls"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__ROOM_ID", "==", roomIdList));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_TYPE", "==", "INPUT"));
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs).get(0);
	}

	/**
	 * Returns room IDs associated with a project.
	 *
	 * @param projectId project identifier
	 * @return room ID list
	 */
	public static List<String> getRoomIdListPerProject(String projectId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ROOM__ROOM_ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__PROJECT_ID", "==", projectId));
		List<String> insightIdList = QueryExecutionUtility.flushToListString(modelInferenceLogsDb, qs);
		return insightIdList;
	}

	/**
	 * Creates a conversation row and returns the generated room/conversation ID.
	 *
	 * @param roomName    display name of the room
	 * @param roomContext room context payload
	 * @param userId      owner user id
	 * @param userName    owner display name
	 * @param userEmail   owner email
	 * @param agentType   model/agent type
	 * @param agentId     model/agent id
	 * @param isActive    active state
	 * @param projectId   project id
	 * @param projectName project name
	 * @return generated conversation/room id
	 */
	public static String doCreateNewConversation(String roomName, String roomContext, String userId, String userName,
			String userEmail, String agentType, String agentId, Boolean isActive, String projectId,
			String projectName) {
		String convoId = GUID.v7().toUUID().toString();
		doCreateNewConversation(convoId, convoId, roomName, roomContext, userId, userName, userEmail, agentType,
				agentId, isActive, projectId, projectName, null, null, null);
		return convoId;
	}

	/**
	 * Creates a conversation row using the insight id as both INSIGHT_ID and
	 * ROOM_ID.
	 *
	 * @param insightId   insight id (also used as room id)
	 * @param roomName    display name of the room
	 * @param roomContext room context payload
	 * @param userId      owner user id
	 * @param userName    owner display name
	 * @param userEmail   owner email
	 * @param agentType   model/agent type
	 * @param agentId     model/agent id
	 * @param isActive    active state
	 * @param projectId   project id
	 * @param projectName project name
	 */
	public static void doCreateNewConversation(String insightId, String roomName, String roomContext, String userId,
			String userName, String userEmail, String agentType, String agentId, Boolean isActive, String projectId,
			String projectName) {

		doCreateNewConversation(insightId, insightId, roomName, roomContext, userId, userName, userEmail, agentType,
				agentId, isActive, projectId, projectName, null, null, null);
	}

	/**
	 * Creates a conversation row with options support.
	 *
	 * @param insightId   insight identifier
	 * @param roomId      room identifier
	 * @param roomName    display name of the room
	 * @param roomContext room context payload
	 * @param userId      owner user id
	 * @param userName    owner display name
	 * @param userEmail   owner email
	 * @param agentType   model/agent type
	 * @param agentId     model/agent id
	 * @param isActive    active state
	 * @param projectId   project id
	 * @param projectName project name
	 * @param options     optional room options map
	 */
	public static void doCreateNewConversation(String insightId, String roomId, String roomName, String roomContext,
			String userId, String userName, String userEmail, String agentType, String agentId, Boolean isActive,
			String projectId, String projectName, Map<String, Object> options) {

		doCreateNewConversation(insightId, insightId, roomName, roomContext, userId, userName, userEmail, agentType,
				agentId, isActive, projectId, projectName, null, null, null);
	}

	/**
	 * Creates a conversation row with optional workspace and options metadata.
	 *
	 * @param insightId   insight identifier
	 * @param roomId      room identifier
	 * @param roomName    display name of the room
	 * @param roomContext room context payload
	 * @param userId      owner user id
	 * @param userName    owner display name
	 * @param userEmail   owner email
	 * @param agentType   model/agent type
	 * @param agentId     model/agent id
	 * @param isActive    active state
	 * @param projectId   project id
	 * @param projectName project name
	 * @param workspaceId workspace id (nullable)
	 * @param options     optional room options map
	 */
	public static void doCreateNewConversation(String insightId, String roomId, String roomName, String roomContext,
			String userId, String userName, String userEmail, String agentType, String agentId, Boolean isActive,
			String projectId, String projectName, String workspaceId, Map<String, Object> options,
			String parentRoomId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "INSERT INTO ROOM (INSIGHT_ID, ROOM_ID, ROOM_NAME, "
				+ "ROOM_CONTEXT, USER_ID, USER_NAME, USER_EMAIL_ID, " + "AGENT_TYPE, AGENT_ID, IS_ACTIVE, "
				+ "DATE_CREATED, PROJECT_ID, PROJECT_NAME, WORKSPACE_ID, OPTIONS, PARENT_ROOM_ID) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		// boolean allowClob =
		// modelInferenceLogsDb.getQueryUtil().allowClobJavaObject();
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, insightId);
			ps.setString(index++, roomId);
			if (roomName != null) {
				ps.setString(index++, roomName);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			if (roomContext != null) {
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps, roomContext, index++, GSON);
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			ps.setString(index++, userId);
			if (userName != null) {
				ps.setString(index++, userName);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			if (userEmail != null) {
				ps.setString(index++, userEmail);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			if (agentType != null) {
				ps.setString(index++, agentType);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			if (agentId != null) {
				ps.setString(index++, agentId);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			ps.setBoolean(index++, isActive);
			ps.setTimestamp(index++, Utility.getCurrentSqlTimestampUTC());
			ps.setString(index++, projectId);
			ps.setString(index++, projectName);
			if (workspaceId != null) {
				ps.setString(index++, workspaceId);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			if (options != null) {
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps, options, index++, GSON);
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			if (parentRoomId != null) {
				ps.setString(index++, parentRoomId);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to create conversation room record for roomId '{}' and userId '{}'.", roomId,
					userId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Checks whether a room exists.
	 *
	 * @param roomId room identifier
	 * @return {@code true} if at least one room row exists
	 */
	public static boolean doCheckRoomExists(String roomId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "SELECT COUNT(*) FROM ROOM WHERE ROOM_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, roomId);
			if (ps.execute()) {
				ResultSet rs = ps.getResultSet();
				if (rs.next()) {
					int count = rs.getInt(1);
					return count >= 1;
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to check whether room exists for roomId '{}'.", roomId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
		return false;
	}

	/**
	 * Checks whether a room exists and belongs to the given user.
	 * <p>
	 * This is the cheapest way to validate room access - it only touches the ROOM
	 * table and never loads the room's messages, builds a {@link Room}, populates
	 * the user's room hash, or takes the room mutation lock. Use this instead of
	 * {@code RoomUtils.getOrLoadRoom} when the caller only needs an access check.
	 * Inactive (closed) rooms are still considered accessible here, matching the
	 * behavior of {@link #getRoomById(String, String)}.
	 * </p>
	 *
	 * @param roomId room identifier
	 * @param userId user identifier
	 * @return {@code true} if the room exists for this user
	 */
	public static boolean doCheckRoomExistsForUser(String roomId, String userId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "SELECT COUNT(*) FROM ROOM WHERE ROOM_ID = ? AND USER_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, roomId);
			ps.setString(index++, userId);
			if (ps.execute()) {
				ResultSet rs = ps.getResultSet();
				if (rs.next()) {
					int count = rs.getInt(1);
					return count >= 1;
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to check whether room exists for roomId '{}' and userId '{}'.", roomId, userId,
					e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
		return false;
	}

	/**
	 * Aggregates token and latency stats from the MESSAGE table for one room.
	 * Token columns are split by row type (INPUT rows carry input/cache tokens,
	 * RESPONSE rows carry output/thinking tokens) and RESPONSE_TIME is duplicated
	 * on both rows of a call, so latency is read from RESPONSE rows only.
	 * <p>
	 * Callers must validate room ownership before calling - this aggregates by
	 * ROOM_ID alone.
	 * </p>
	 *
	 * @param roomId room identifier
	 * @return stats map; {@code available=false} when no rows were found
	 */
	public static Map<String, Object> getRoomTokenAndLatencyStats(String roomId) {
		Map<String, Object> stats = new HashMap<>();
		stats.put("available", false);
		if (roomId == null || roomId.isBlank()) {
			return stats;
		}
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "SELECT MESSAGE_TYPE, COUNT(MESSAGE_ID), SUM(INPUT_TOKENS), SUM(OUTPUT_TOKENS), "
				+ "SUM(THINKING_TOKENS), SUM(CACHE_READ_TOKENS), SUM(CACHE_CREATION_TOKENS), "
				+ "SUM(RESPONSE_TIME), AVG(RESPONSE_TIME), MAX(RESPONSE_TIME) "
				+ "FROM MESSAGE WHERE ROOM_ID = ? GROUP BY MESSAGE_TYPE";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			ps.setString(1, roomId);
			if (ps.execute()) {
				ResultSet rs = ps.getResultSet();
				while (rs.next()) {
					String messageType = rs.getString(1);
					if ("RESPONSE".equalsIgnoreCase(messageType)) {
						stats.put("available", true);
						stats.put("llmCalls", rs.getLong(2));
						stats.put("outputTokens", rs.getLong(4));
						stats.put("thinkingTokens", rs.getLong(5));
						stats.put("totalResponseTimeMs", rs.getDouble(8));
						stats.put("avgResponseTimeMs", Math.round(rs.getDouble(9) * 100.0) / 100.0);
						stats.put("maxResponseTimeMs", rs.getDouble(10));
					} else if ("INPUT".equalsIgnoreCase(messageType)) {
						stats.put("available", true);
						stats.put("inputTokens", rs.getLong(3));
						stats.put("cacheReadTokens", rs.getLong(6));
						stats.put("cacheCreationTokens", rs.getLong(7));
					}
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to aggregate inference stats for roomId '{}'.", roomId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
		return stats;
	}

	/**
	 * Checks whether a message exists in a room (used by message-id migration
	 * validation).
	 *
	 * @param roomId    room identifier
	 * @param messageId message identifier
	 * @return {@code true} when the room/message pair exists
	 */
	public static boolean doCheckMessageIdMigration(String roomId, String messageId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "SELECT COUNT(*) FROM MESSAGE WHERE ROOM_ID = ? AND MESSAGE_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, roomId);
			ps.setString(index++, messageId);
			ps.execute();
			if (ps.execute()) {
				ResultSet rs = ps.getResultSet();
				if (rs.next()) {
					int count = rs.getInt(1);
					return count >= 1;
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to validate message migration for roomId '{}' and messageId '{}'.", roomId,
					messageId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
		return false;
	}

	/**
	 * Checks whether an agent/model id exists in the AGENT table.
	 *
	 * @param agentId agent identifier
	 * @return {@code true} if the agent is registered
	 */
	public static boolean doModelIsRegistered(String agentId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "SELECT COUNT(*) FROM AGENT WHERE AGENT_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, agentId);
			ps.execute();
			if (ps.execute()) {
				ResultSet rs = ps.getResultSet();
				if (rs.next()) {
					int count = rs.getInt(1);
					return count >= 1;
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to check whether agent is registered for agentId '{}'.", agentId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
		return false;
	}

	/**
	 * Creates an agent row with a generated id.
	 *
	 * @param agentName        agent display name
	 * @param agentDescription description text
	 * @param agentType        agent/model type
	 * @param author           author identifier
	 * @return generated agent id
	 */
	public static String doCreateNewAgent(String agentName, String agentDescription, String agentType, String author) {
		String agentId = GUID.v7().toUUID().toString();
		doCreateNewAgent(agentId, agentName, agentDescription, agentType, author);
		return agentId;
	}

	/**
	 * Creates an agent row with an explicit id.
	 *
	 * @param agentId          agent identifier
	 * @param agentName        agent display name
	 * @param agentDescription description text
	 * @param agentType        agent/model type
	 * @param author           author identifier
	 */
	public static void doCreateNewAgent(String agentId, String agentName, String agentDescription, String agentType,
			String author) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "INSERT INTO AGENT (AGENT_ID, AGENT_NAME, DESCRIPTION, AGENT_TYPE, "
				+ "AUTHOR, DATE_CREATED) VALUES (?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, agentId);
			ps.setString(index++, agentName);
			ps.setString(index++, agentDescription);
			ps.setString(index++, agentType);
			ps.setString(index++, author);
			ps.setTimestamp(index++, Utility.getCurrentSqlTimestampUTC());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to create agent record for agentId '{}'.", agentId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Records a message using the current time and room id derived from insight id.
	 *
	 * @param messageId     message id
	 * @param messageType   message type
	 * @param messageData   serialized message payload
	 * @param messageMethod message method (for example, {@code ask})
	 * @param tokenSize     token count
	 * @param reponseTime   model response time
	 * @param agentId       agent/model id
	 * @param insightId     insight id
	 * @param sessionId     session id
	 * @param userId        user id
	 * @param userName      user display name
	 * @param userEmail     user email
	 */
	public static void doRecordMessage(String messageId, String messageType, String messageData, String messageMethod,
			Integer tokenSize, Double reponseTime, String agentId, String insightId, String sessionId, String userId,
			String userName, String userEmail) {
		ZonedDateTime dateCreated = ZonedDateTime.now();
		doRecordMessage(messageId, null, messageType, messageData, messageMethod, tokenSize, null, null, null, null,
				null, reponseTime, dateCreated, agentId, insightId, sessionId, insightId, // roomId
				userId, userName, userEmail);
	}

	/**
	 * Records a message using an explicit created timestamp.
	 *
	 * @param messageId     message id
	 * @param messageType   message type
	 * @param messageData   serialized message payload
	 * @param messageMethod message method
	 * @param tokenSize     token count
	 * @param reponseTime   model response time
	 * @param dateCreated   created time
	 * @param agentId       agent/model id
	 * @param insightId     insight id
	 * @param sessionId     session id
	 * @param roomId        room id
	 * @param userId        user id
	 * @param userName      user display name
	 * @param userEmail     user email
	 */
	public static void doRecordMessage(String messageId, String messageType, String messageData, String messageMethod,
			Integer tokenSize, Double reponseTime, ZonedDateTime dateCreated, String agentId, String insightId,
			String sessionId, String roomId, String userId, String userName, String userEmail) {
		doRecordMessage(messageId, null, messageType, messageData, messageMethod, tokenSize, null, null, null, null,
				null, reponseTime, dateCreated, agentId, insightId, sessionId, insightId, // roomId
				userId, userName, userEmail);
	}

	/**
	 * Records a message row with full metadata, including optional transaction id.
	 *
	 * @param messageId     message id
	 * @param transactionId transaction id (nullable)
	 * @param messageType   message type
	 * @param messageData   serialized message payload
	 * @param messageMethod message method
	 * @param tokenSize     token count
	 * @param reponseTime   model response time
	 * @param dateCreated   created time
	 * @param agentId       agent/model id
	 * @param insightId     insight id
	 * @param sessionId     session id
	 * @param roomId        room id
	 * @param userId        user id
	 * @param userName      user display name
	 * @param userEmail     user email
	 */
	public static void doRecordMessage(String messageId, String transactionId, String messageType, String messageData,
			String messageMethod, Integer tokenSize, Double reponseTime, ZonedDateTime dateCreated, String agentId,
			String insightId, String sessionId, String roomId, String userId, String userName, String userEmail) {
		doRecordMessage(messageId, transactionId, messageType, messageData, messageMethod, tokenSize, null, null, null,
				null, null, reponseTime, dateCreated, agentId, insightId, sessionId, roomId, userId, userName,
				userEmail);
	}

	/**
	 * Records a message row with granular, nullable per-transaction token counts.
	 * THINKING_TOKENS is intended for the RESPONSE row only (assistant output);
	 * pass null for the INPUT row.
	 */
	public static void doRecordMessage(String messageId, String transactionId, String messageType, String messageData,
			String messageMethod, Integer tokenSize, Integer inputTokens, Integer outputTokens, Integer cacheReadTokens,
			Integer cacheCreationTokens, Integer thinkingTokens, Double reponseTime, ZonedDateTime dateCreated,
			String agentId, String insightId, String sessionId, String roomId, String userId, String userName,
			String userEmail) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		// convert the time to UTC
		ZonedDateTime dateCreatedUTC = Utility.convertZonedDateTimeToUTC(dateCreated);

		// boolean allowClob =
		// modelInferenceLogsDb.getQueryUtil().allowClobJavaObject();
		String query = "INSERT INTO MESSAGE (MESSAGE_ID, TRANSACTION_ID, MESSAGE_TYPE, MESSAGE_DATA, MESSAGE_METHOD, MESSAGE_TOKENS,"
				+ " INPUT_TOKENS, OUTPUT_TOKENS, CACHE_READ_TOKENS, CACHE_CREATION_TOKENS, THINKING_TOKENS, RESPONSE_TIME,"
				+ " DATE_CREATED, AGENT_ID, INSIGHT_ID, ROOM_ID, SESSIONID, USER_ID, USER_NAME, USER_EMAIL_ID) "
				+ "	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, messageId);
			if (transactionId != null) {
				ps.setString(index++, transactionId);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			ps.setString(index++, messageType);
			if (messageData != null) {
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfBlob(ps.getConnection(), ps, messageData, index++);
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			ps.setString(index++, messageMethod);
			if (tokenSize != null) {
				ps.setInt(index++, tokenSize);
			} else {
				ps.setNull(index++, java.sql.Types.INTEGER);
			}
			if (inputTokens != null) {
				ps.setInt(index++, inputTokens);
			} else {
				ps.setNull(index++, java.sql.Types.INTEGER);
			}
			if (outputTokens != null) {
				ps.setInt(index++, outputTokens);
			} else {
				ps.setNull(index++, java.sql.Types.INTEGER);
			}
			if (cacheReadTokens != null) {
				ps.setInt(index++, cacheReadTokens);
			} else {
				ps.setNull(index++, java.sql.Types.INTEGER);
			}
			if (cacheCreationTokens != null) {
				ps.setInt(index++, cacheCreationTokens);
			} else {
				ps.setNull(index++, java.sql.Types.INTEGER);
			}
			if (thinkingTokens != null) {
				ps.setInt(index++, thinkingTokens);
			} else {
				ps.setNull(index++, java.sql.Types.INTEGER);
			}
			if (reponseTime != null) {
				ps.setDouble(index++, reponseTime);
			} else {
				ps.setNull(index++, java.sql.Types.DOUBLE);
			}
			ps.setTimestamp(index++, java.sql.Timestamp.valueOf(dateCreatedUTC.toLocalDateTime()));
			ps.setString(index++, agentId);
			ps.setString(index++, insightId);
			ps.setString(index++, roomId);
			ps.setString(index++, sessionId);
			ps.setString(index++, userId);
			if (userName != null) {
				ps.setString(index++, userName);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			if (userEmail != null) {
				ps.setString(index++, userEmail);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to record message '{}' for roomId '{}'.", messageId, roomId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Marks a room inactive for a specific user.
	 *
	 * @param userId user identifier
	 * @param roomId room identifier
	 * @return {@code true} when the update succeeds
	 */
	public static boolean doSetRoomToInactive(String userId, String roomId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		try {
			PreparedStatement ps = modelInferenceLogsDb
					.getPreparedStatement("UPDATE ROOM SET IS_ACTIVE=? WHERE USER_ID=? AND ROOM_ID=?");
			if (ps == null) {
				throw new IllegalArgumentException("Error generating prepared statement to set room inactive");
			}
			try {
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, false);
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, roomId);
				ps.executeUpdate();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (Exception e) {
				classLogger.error("Failed to set room inactive for userId '{}' and roomId '{}'.", userId, roomId, e);
				throw e;
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, ps);
			}
		} catch (Exception e) {
			classLogger.error("Room deactivation flow failed for userId '{}' and roomId '{}'.", userId, roomId, e);
			return false;
		}
		return true;
	}

	/**
	 * Checks whether a room is inactive for a specific user.
	 *
	 * @param userId user identifier
	 * @param roomId room identifier
	 * @return {@code true} if the room is inactive
	 */
	public static boolean isRoomInActive(String userId, String roomId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ROOM__IS_ACTIVE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__ROOM_ID", "==", roomId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ROOM__IS_ACTIVE", "==", false, PixelDataType.BOOLEAN));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Failed to check inactive state for roomId '{}' and userId '{}'.", roomId, userId, e);
		}
		return false;
	}

	/**
	 * Updates the pinned state for a room.
	 *
	 * @param userId user identifier
	 * @param roomId room identifier
	 * @param pinned new pinned state
	 * @return {@code true} when the update succeeds
	 */
	public static boolean doSetRoomToPinned(String userId, String roomId, boolean pinned) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		try {
			PreparedStatement ps = modelInferenceLogsDb
					.getPreparedStatement("UPDATE ROOM SET PINNED=? WHERE USER_ID=? AND ROOM_ID=?");
			if (ps == null) {
				throw new IllegalArgumentException("Error generating prepared statement to set room pinned");
			}
			try {
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, pinned);
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, roomId);
				ps.executeUpdate();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (Exception e) {
				classLogger.error("Failed to update pinned state for roomId '{}' and userId '{}'.", roomId, userId, e);
				throw e;
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, ps);
			}
		} catch (Exception e) {
			classLogger.error("Room pin update flow failed for roomId '{}' and userId '{}'.", roomId, userId, e);
			return false;
		}
		return true;
	}

	/**
	 * Searches messages for a user and project by keyword. Handles message_data as
	 * a binary field (bytea/blob/varbinary). Converts/casts as necessary for each
	 * DB so text search via LIKE is possible.
	 *
	 * @param userId    the user to search for
	 * @param projectId the project to search within
	 * @param keyword   the text keyword to find in message bodies
	 * @return a list of matching messages (room_id, message_text, message_id)
	 */
	public static List<Map<String, Object>> searchMessages(String userId, String projectId, String keyword) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();

		// Always select room_id and message_id
		qs.addSelector(new QueryColumnSelector("ROOM__ROOM_ID", "room_id"));
		qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_ID", "message_id"));

		// Build a selector for message_text out of message_data, adapted to DB type
		QueryFunctionSelector messageTextSelector = modelInferenceLogsDb.getQueryUtil()
				.getBlobToStringFunctionSelector(new QueryColumnSelector("MESSAGE__MESSAGE_DATA"), "message_text");
		qs.addSelector(messageTextSelector);

		// JOIN, filters, and ordering
		qs.addRelation("MESSAGE__ROOM_ID", "ROOM__ROOM_ID", "left.join");
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ROOM__IS_ACTIVE", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__PROJECT_ID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));

		// Add filter on decoded message text
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(messageTextSelector, // use the computed selector (the
																						// decoded/casted field)
				"?like", keyword.toLowerCase(), // (may want '?ilike' if framework supports, for case-insensitive)
				PixelDataType.CONST_STRING));

		qs.addOrderBy("ROOM__DATE_CREATED", "DESC");
		qs.addOrderBy("MESSAGE__DATE_CREATED", "DESC");

		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Updates a room's display name for a specific user.
	 *
	 * @param userId   user identifier
	 * @param roomId   room identifier
	 * @param roomName new room name
	 * @return {@code true} when the update succeeds
	 */
	public static boolean doSetNameForRoom(String userId, String roomId, String roomName) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		try {
			PreparedStatement ps = modelInferenceLogsDb
					.getPreparedStatement("UPDATE ROOM SET ROOM_NAME=? WHERE USER_ID=? AND ROOM_ID=?");
			if (ps == null) {
				throw new IllegalArgumentException("Error generating prepared statement to set room name");
			}
			try {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, roomName);
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, roomId);
				ps.executeUpdate();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (Exception e) {
				classLogger.error("Failed to update room name for roomId '{}' and userId '{}'.", roomId, userId, e);
				throw e;
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, ps);
			}
		} catch (Exception e) {
			classLogger.error("Room rename flow failed for roomId '{}' and userId '{}'.", roomId, userId, e);
			return false;
		}
		return true;
	}

	/**
	 * Returns the current display name for a user's room.
	 *
	 * @param userId user identifier
	 * @param roomId room identifier
	 * @return current room name, or {@code null} when the room does not exist or
	 *         has no name set
	 */
	public static String doGetRoomName(String userId, String roomId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "SELECT ROOM_NAME FROM ROOM WHERE USER_ID = ? AND ROOM_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, userId);
			ps.setString(index++, roomId);
			if (ps.execute()) {
				ResultSet rs = ps.getResultSet();
				if (rs.next()) {
					return rs.getString(1);
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to get room name for roomId '{}' and userId '{}'.", roomId, userId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
		return null;
	}

	/**
	 * Updates a room's display name only when the current name is still unset or
	 * still equal to the auto-derived default (the truncated initial request set at
	 * room creation). A custom name set by the user is never overwritten.
	 *
	 * @param userId      user identifier
	 * @param roomId      room identifier
	 * @param roomName    new room name
	 * @param defaultName auto-derived name that is allowed to be replaced
	 * @return {@code true} when a row was updated
	 */
	public static boolean doSetNameForRoomIfDefault(String userId, String roomId, String roomName, String defaultName) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "UPDATE ROOM SET ROOM_NAME=? WHERE USER_ID=? AND ROOM_ID=? "
				+ "AND (ROOM_NAME IS NULL OR ROOM_NAME='' OR ROOM_NAME=?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, roomName);
			ps.setString(index++, userId);
			ps.setString(index++, roomId);
			ps.setString(index++, defaultName);
			int rows = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			return rows > 0;
		} catch (Exception e) {
			classLogger.error("Failed to conditionally update room name for roomId '{}' and userId '{}'.", roomId,
					userId, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, ps);
		}
	}

	/**
	 * Returns ask-message history for a room/user sorted by creation date.
	 *
	 * @param userId   user identifier
	 * @param roomId   room identifier
	 * @param dateSort sort direction ({@code ASC} or {@code DESC})
	 * @return message history rows
	 */
	public static List<Map<String, Object>> doRetrieveConversation(String userId, String roomId, String dateSort) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = retrieveMessageQS(userId, roomId, dateSort);
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Returns paginated ask-message history for a room/user.
	 *
	 * @param userId   user identifier
	 * @param roomId   room identifier
	 * @param dateSort sort direction ({@code ASC} or {@code DESC})
	 * @param limit    max rows to return
	 * @param offset   rows to skip
	 * @return message history rows
	 */
	public static List<Map<String, Object>> doRetrieveConversation(String userId, String roomId, String dateSort,
			Integer limit, Integer offset) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = retrieveMessageQS(userId, roomId, dateSort);
		qs.setLimit(limit);
		qs.setOffSet(offset);
		List<Map<String, Object>> response = QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
		if (dateSort.equals("DESC")) {
			Collections.reverse(response);
		}
		return response;
	}

	/**
	 * Builds the base query for retrieving ask-message history.
	 *
	 * @param userId   user identifier
	 * @param roomId   room identifier
	 * @param dateSort sort direction ({@code ASC} or {@code DESC})
	 * @return configured query struct
	 */
	private static SelectQueryStruct retrieveMessageQS(String userId, String roomId, String dateSort) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("MESSAGE__DATE_CREATED"));
		qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_TYPE"));
		qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_DATA"));
		qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_ID"));
		qs.addSelector(new QueryColumnSelector("FEEDBACK__RATING"));
		qs.addSelector(new QueryColumnSelector("FEEDBACK__FEEDBACK_TEXT"));

		qs.addRelation("MESSAGE__MESSAGE_ID", "FEEDBACK__MESSAGE_ID", "left.join");
		qs.addRelation("MESSAGE__MESSAGE_TYPE", "FEEDBACK__MESSAGE_TYPE", "left.join");

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__ROOM_ID", "==", roomId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__USER_ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_METHOD", "==", "ask"));
		qs.addOrderBy(new QueryColumnOrderBySelector("MESSAGE__DATE_CREATED", dateSort));
		return qs;
	}

	/**
	 * Returns nearest-neighbor message history for a room/user.
	 *
	 * @param userId   user identifier
	 * @param roomId   room identifier
	 * @param dateSort sort direction ({@code ASC} or {@code DESC})
	 * @return nearest-neighbor message rows
	 */
	public static List<Map<String, Object>> doRetrieveNearestNeighbor(String userId, String roomId, String dateSort) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("MESSAGE__DATE_CREATED"));
		qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_TYPE"));
		qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_DATA"));
		qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_ID"));
		qs.addSelector(new QueryColumnSelector("FEEDBACK__RATING"));
		qs.addSelector(new QueryColumnSelector("FEEDBACK__FEEDBACK_TEXT"));

		qs.addRelation("MESSAGE__MESSAGE_ID", "FEEDBACK__MESSAGE_ID", "left.join");
		qs.addRelation("MESSAGE__MESSAGE_TYPE", "FEEDBACK__MESSAGE_TYPE", "left.join");

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__ROOM_ID", "==", roomId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__USER_ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_METHOD", "==", "nearestNeighbor"));
		qs.addOrderBy(new QueryColumnOrderBySelector("MESSAGE__DATE_CREATED", dateSort));
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Verifies that a room belongs to a user and returns room/project identifiers.
	 *
	 * @param userId user identifier
	 * @param roomId room identifier
	 * @return verification rows (empty when invalid)
	 */
	public static List<Map<String, Object>> doVerifyConversation(String userId, String roomId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ROOM__ROOM_ID"));
		qs.addSelector(new QueryColumnSelector("ROOM__PROJECT_ID"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__ROOM_ID", "==", roomId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));
		qs.setDistinct(true);
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Retrieves conversation rooms for a user with optional filtering, sorting, and
	 * paging.
	 * <p>
	 * Only active rooms are returned, and each room must have at least one stored
	 * message row with non-null message content.
	 * </p>
	 *
	 * @param userId    user identifier used to scope rooms
	 * @param projectId optional project identifier to further scope rooms; when
	 *                  {@code null}, rooms across all projects are eligible
	 * @param limit     maximum number of rooms to return; values {@code <= 0}
	 *                  disable limiting
	 * @param offset    number of rows to skip before collecting results; values
	 *                  {@code <= 0} disable offset paging
	 * @param sortDir   sort direction for {@code DATE_CREATED}; accepted values are
	 *                  {@code ASC} and {@code DESC} (any other value is treated as
	 *                  {@code DESC})
	 * @param search    optional room-name contains filter (case-insensitive
	 *                  {@code LIKE}); {@code null}/blank disables search filtering
	 * @param pinned    optional pinned-state filter; {@code true} returns only
	 *                  pinned rooms, {@code false} returns unpinned rooms
	 *                  (including {@code null} pinned values), and {@code null}
	 *                  disables pinned filtering
	 * @return a list of room records, where each map contains the selected room
	 *         fields for that row:
	 *         <ul>
	 *         <li>{@code ROOM_ID} (or aliased header for room id)</li>
	 *         <li>{@code ROOM_NAME} (or aliased header for room name)</li>
	 *         <li>{@code DATE_CREATED} (or aliased header for room create
	 *         timestamp)</li>
	 *         <li>{@code PINNED} (or aliased header for pinned state)</li>
	 *         <li>{@code WORKSPACE_ID} (or aliased header for workspace link)</li>
	 *         </ul>
	 */
	public static List<Map<String, Object>> getUserConversations(String userId, String projectId, long limit,
			long offset, String sortDir, String search, Boolean pinned) {
		return getUserConversations(userId, projectId, limit, offset, sortDir, search, pinned, null);
	}

	public static List<Map<String, Object>> getUserConversations(String userId, String projectId, long limit,
			long offset, String sortDir, String search, Boolean pinned, String roomOptionsSearch) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ROOM__ROOM_ID"));
		qs.addSelector(new QueryColumnSelector("ROOM__ROOM_NAME"));
		qs.addSelector(new QueryColumnSelector("ROOM__DATE_CREATED"));
		qs.addSelector(new QueryColumnSelector("ROOM__PINNED"));
		qs.addSelector(new QueryColumnSelector("ROOM__WORKSPACE_ID"));

		// Subquery to filter only rooms that are active and fit query restraints
		SelectQueryStruct subQs = new SelectQueryStruct();
		subQs.addSelector(new QueryColumnSelector("ROOM__ROOM_ID"));
		subQs.addRelation("ROOM__ROOM_ID", "MESSAGE__ROOM_ID", "inner.join");
		subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));
		subQs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ROOM__IS_ACTIVE", "==", true, PixelDataType.BOOLEAN));
		subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_DATA", "!=", null));
		// Exclude subagent rooms -- these are real Room rows (their roomId is the
		// subagent's runId) created purely as a spawned subagent's private
		// workspace, not user-initiated conversations. See AgentSubAgentRegistry.
		subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__PARENT_ROOM_ID", "==", null));
		if (projectId != null) {
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__PROJECT_ID", "==", projectId));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ROOM__ROOM_ID", "==", subQs));

		// SEARCH
		if (search != null && !search.trim().isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__ROOM_NAME", "?like", "%" + search + "%",
					PixelDataType.CONST_STRING));
		}

		// ROOM OPTIONS SEARCH - free-text substring match on the OPTIONS text column.
		// Portable across H2 and PostgreSQL since OPTIONS is stored as plain text.
		if (roomOptionsSearch != null && !roomOptionsSearch.trim().isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__OPTIONS", "?like",
					"%" + roomOptionsSearch.trim() + "%", PixelDataType.CONST_STRING));
		}

		// PINNED filter
		// when pinned == true -> only rooms with PINNED == true
		// when pinned == false -> rooms with PINNED == false OR PINNED IS NULL (treat
		// unset as not pinned)
		if (pinned != null) {
			if (pinned.booleanValue()) {
				qs.addExplicitFilter(
						SimpleQueryFilter.makeColToValFilter("ROOM__PINNED", "==", true, PixelDataType.BOOLEAN));
			} else {
				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__PINNED", "==",
						Arrays.asList(false, null), PixelDataType.BOOLEAN));
			}
		}

		// LIMIT/OFFSET
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		// SORTING
		sortDir = (sortDir != null) ? sortDir.trim().toUpperCase() : "DESC";
		qs.addOrderBy(new QueryColumnOrderBySelector("ROOM__DATE_CREATED", sortDir));
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Removes feedback for a message.
	 *
	 * @param messageId message identifier
	 */
	public static void removeFeedback(String messageId) {
		if (!feedbackExists(messageId)) {
			throw new SemossPixelException("No feedback found for the given messageId to remove.");
		}
		deleteFeedbackEntry(messageId);
	}

	/**
	 * Retrieves the stored room context for a user/room pair.
	 *
	 * @param userId user identifier
	 * @param roomId room identifier
	 * @return room context rows
	 */
	public static List<Map<String, Object>> getRoomContext(String userId, String roomId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "ROOM_CONTEXT"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ROOM_TABLE_NAME + "ROOM_ID", "==", roomId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ROOM_TABLE_NAME + "USER_ID", "==", userId));

		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Retrieves the stored room options for a user/room pair.
	 *
	 * @param roomId room identifier
	 * @param userId user identifier
	 * @return room options rows
	 */
	public static List<Map<String, Object>> getRoomOptions(String roomId, String userId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ROOM__OPTIONS"));
		// Also return the persisted room name so callers (e.g. the playground
		// room breadcrumb) can display it on load without a separate query.
		qs.addSelector(new QueryColumnSelector("ROOM__ROOM_NAME"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__ROOM_ID", "==", roomId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));

		Set<String> mapKeys = new HashSet<>();
		mapKeys.add("OPTIONS");

		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs, mapKeys);
	}

	/**
	 * Updates room options for a user/room pair.
	 *
	 * @param roomId  room identifier
	 * @param userId  user identifier
	 * @param options options map (nullable)
	 */
	public static void setRoomOptions(String roomId, String userId, Map<String, Object> options) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "UPDATE ROOM SET OPTIONS = ? WHERE USER_ID = ? AND ROOM_ID = ?";

		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			if (options != null) {
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps, options, index++, GSON);
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			ps.setString(index++, userId);
			ps.setString(index++, roomId);
			ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update room options for roomId '{}' and userId '{}'.", roomId, userId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Updates the workspace association for a room.
	 *
	 * @param roomId      room identifier
	 * @param userId      user identifier
	 * @param workspaceId workspace identifier (nullable)
	 */
	public static void setRoomWorkspaceId(String roomId, String userId, String workspaceId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "UPDATE ROOM SET WORKSPACE_ID = ? WHERE USER_ID = ? AND ROOM_ID = ?";

		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			if (workspaceId != null) {
				ps.setString(index++, workspaceId);
				;
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			ps.setString(index++, userId);
			ps.setString(index++, roomId);
			ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to set workspaceId for roomId '{}' and userId '{}'.", roomId, userId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Updates room context for a user/room pair.
	 *
	 * @param roomId  room identifier
	 * @param userId  user identifier
	 * @param context room context payload
	 */
	public static void setRoomContext(String roomId, String userId, String context) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		try {
			PreparedStatement ps = modelInferenceLogsDb
					.getPreparedStatement("UPDATE ROOM SET ROOM_CONTEXT=? WHERE USER_ID=? AND ROOM_ID=?");
			if (ps == null) {
				throw new IllegalArgumentException("Error generating prepared statement to set room context");
			}
			try {
				int parameterIndex = 1;
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps, context, parameterIndex++, GSON);
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, roomId);
				ps.executeUpdate();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (Exception e) {
				classLogger.error("Failed to update room context for roomId '{}' and userId '{}'.", roomId, userId, e);
				throw e;
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, ps);
			}
		} catch (Exception e) {
			classLogger.error("Room context update flow failed for roomId '{}' and userId '{}'.", roomId, userId, e);
		}
	}

	/**
	 * Deletes the feedback row tied to a message id.
	 *
	 * @param messageId message identifier
	 */
	private static void deleteFeedbackEntry(String messageId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String deleteQuery = "DELETE FROM FEEDBACK WHERE MESSAGE_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(deleteQuery);
			int index = 1;
			ps.setString(index++, messageId);
			int affectedRows = ps.executeUpdate();
			if (affectedRows == 0) {
				classLogger.warn(
						"No changes made while attempting to delete feedback for MESSAGE_ID: {}. Please verify the state of the feedback.",
						messageId);
			}
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to delete feedback entry for messageId '{}'.", messageId, e);
			throw new SemossPixelException("Error while deleting feedback: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Calculates total token usage or response time for one user and one engine for
	 * a frequency window.
	 *
	 * @param restrictionMode usage mode ({@code MAXTOKENS} or
	 *                        {@code MAXRESPONSETIME})
	 * @param user            user to evaluate
	 * @param engineId        engine identifier
	 * @param currentDateTime reference date/time
	 * @param frequency       window frequency ({@code DAY}, {@code WEEK},
	 *                        {@code MONTH}, {@code YEAR}, {@code ALL_TIME})
	 * @return aggregate usage value, or {@code null} if unavailable
	 */
	public static Number getTotalTokensOrTotalResponseTime(String restrictionMode, User user, String engineId,
			ZonedDateTime currentDateTime, String frequency) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		if (restrictionMode == null) {
			throw new IllegalArgumentException("Must pass in a valid restriction mode");
		}

		// Get the date range based on the frequency specification
		// Supports: WEEK, MONTH, YEAR, ALL_TIME
		Map<String, ZonedDateTime> dates = ModelUsageRestrictionUtility.getDateRangeFromFrequency(frequency,
				currentDateTime);

		// Extract start and end dates from the map
		ZonedDateTime startDate = dates.get("start");
		ZonedDateTime endDate = dates.get("end");

		String sumColumn = null;
		if (restrictionMode.equalsIgnoreCase(Constants.MODEL_TOKEN_RESTRICTION_VALUE)) {
			sumColumn = " SUM(MESSAGE_TOKENS) ";
		} else if (restrictionMode.equalsIgnoreCase(Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE)) {
			sumColumn = " SUM(RESPONSE_TIME) ";
		}

		// SQL query to fetch the total tokens or response time
		String query = "SELECT " + sumColumn
				+ " AS \"current_usage\" FROM MESSAGE WHERE USER_ID=? AND AGENT_ID=? AND DATE_CREATED BETWEEN ? AND ?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int psIndex = 1;
			ps.setString(psIndex++, user.getAccessToken(user.getLogins().get(0)).getId());
			ps.setString(psIndex++, engineId);
			ps.setTimestamp(psIndex++, java.sql.Timestamp.valueOf(startDate.toLocalDateTime()));
			ps.setTimestamp(psIndex++, java.sql.Timestamp.valueOf(endDate.toLocalDateTime()));

			RawRDBMSSelectWrapper wrapper = RawRDBMSSelectWrapper.directExecutionPreparedStatement(modelInferenceLogsDb,
					ps.getConnection(), ps, query, false);

			if (wrapper.hasNext()) {
				Number retNum = (Number) wrapper.next().getValues()[0];
				// if this is null
				// that means there are no logs currently for this model
				// we will treat this as 0 usage
				if (retNum == null) {
					return 0;
				}
				return retNum;
			}
		} catch (Exception e) {
			classLogger.error(
					"Failed to calculate usage for userId '{}', engineId '{}', restrictionMode '{}', frequency '{}'.",
					user.getAccessToken(user.getLogins().get(0)).getId(), engineId, restrictionMode, frequency, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, rs);
		}
		return null;
	}

	/**
	 * Calculates total token usage or response time for a user across engines while
	 * excluding restricted engines.
	 *
	 * @param restrictionMode usage mode ({@code MAXTOKENS} or
	 *                        {@code MAXRESPONSETIME})
	 * @param user            user to evaluate
	 * @param engineId        current engine id used to derive exclusions
	 * @param currentDateTime reference date/time
	 * @param frequency       window frequency ({@code DAY}, {@code WEEK},
	 *                        {@code MONTH}, {@code YEAR}, {@code ALL_TIME})
	 * @return aggregate usage value, or {@code null} if unavailable
	 */
	public static Number getTotalUsageForUser(String restrictionMode, User user, String engineId,
			ZonedDateTime currentDateTime, String frequency) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		if (restrictionMode == null) {
			throw new IllegalArgumentException("Must pass in a valid restriction mode");
		}

		// Step 1: Get the list of Engine IDs with MAXRESPONSETIME or MAXTOKENS for the
		// specific user
		List<String> engineIdExcludeList = SecurityEngineUtils.getModelEngineIdsWithRestrictions(user, engineId);
		String excludePSString = "";
		if (engineIdExcludeList != null && !engineIdExcludeList.isEmpty()) {
			StringBuilder excludeSB = new StringBuilder("AND AGENT_ID NOT IN (");
			for (int i = 0; i < engineIdExcludeList.size(); i++) {
				if (i > 0) {
					excludeSB.append(",");
				}
				excludeSB.append("?");
			}
			excludeSB.append(")");
			excludePSString = excludeSB.toString();
		}

		// Step 2: Get the date range based on the frequency specification
		// Supports: WEEK, MONTH, YEAR, ALL_TIME
		Map<String, ZonedDateTime> dates = ModelUsageRestrictionUtility.getDateRangeFromFrequency(frequency,
				currentDateTime);
		// Extract start and end dates from the map
		ZonedDateTime startDate = dates.get("start");
		ZonedDateTime endDate = dates.get("end");

		// Step 3: Determine which column to sum (tokens or response time) based on
		// restrictionMode
		String sumColumn = null;
		if (restrictionMode.equalsIgnoreCase(Constants.MODEL_TOKEN_RESTRICTION_VALUE)) {
			sumColumn = " SUM(MESSAGE_TOKENS) ";
		} else if (restrictionMode.equalsIgnoreCase(Constants.MODEL_COMPUTE_TIME_RESTRICTION_VALUE)) {
			sumColumn = " SUM(RESPONSE_TIME) ";
		}

		// Step 4: Get total usage for the user excluding the engines in the
		// engineIdList
		String query = "SELECT " + sumColumn
				+ " AS \"current_usage\" FROM MESSAGE WHERE USER_ID=? AND DATE_CREATED BETWEEN ? AND ? "
				+ excludePSString;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int psIndex = 1;
			ps.setString(psIndex++, user.getAccessToken(user.getLogins().get(0)).getId());
			ps.setDate(psIndex++, java.sql.Date.valueOf(startDate.toLocalDate()));
			ps.setDate(psIndex++, java.sql.Date.valueOf(endDate.toLocalDate()));
			if (engineIdExcludeList != null && !engineIdExcludeList.isEmpty()) {
				for (String excludeEngineId : engineIdExcludeList) {
					ps.setString(psIndex++, excludeEngineId);
				}
			}

			RawRDBMSSelectWrapper wrapper = RawRDBMSSelectWrapper.directExecutionPreparedStatement(modelInferenceLogsDb,
					ps.getConnection(), ps, query, false);

			if (wrapper.hasNext()) {
				Number retNum = (Number) wrapper.next().getValues()[0];
				// if this is null
				// that means there are no logs currently for this model
				// we will treat this as 0 usage
				if (retNum == null) {
					return 0;
				}
				return retNum;
			}
		} catch (Exception e) {
			classLogger.error("Failed to calculate total usage for userId '{}', restrictionMode '{}', frequency '{}'.",
					user.getAccessToken(user.getLogins().get(0)).getId(), restrictionMode, frequency, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, rs);
		}

		return null;
	}

	/* -------- ROOM PIECES ------- */

	/**
	 * Updates persisted room message history and timestamp.
	 *
	 * @param roomId         room identifier
	 * @param userId         user identifier
	 * @param messageHistory serialized message history payload
	 * @return {@code true} when at least one row is updated
	 */
	public static boolean llm2_updateRoomMessages(String roomId, String userId, String messageHistory) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement updateStmt = null;
		try {
			// Update messages and timestamp where room and user match
			String query = "UPDATE ROOM SET MESSAGES = ?, UPDATED_AT = ? WHERE ROOM_ID = ? AND USER_ID = ?";
			updateStmt = modelInferenceLogsDb.getPreparedStatement(query);

			// Prepare statement
			updateStmt.setString(1, messageHistory);
			updateStmt.setTimestamp(2, Utility.getCurrentSqlTimestampUTC());
			updateStmt.setString(3, roomId);
			updateStmt.setString(4, userId);

			// Execute update
			int rows = updateStmt.executeUpdate();
			if (!updateStmt.getConnection().getAutoCommit()) {
				updateStmt.getConnection().commit();
			}
			return rows > 0;

		} catch (Exception e) {
			classLogger.error("Failed to update room messages for roomId '{}' and userId '{}'.", roomId, userId, e);
			throw new IllegalArgumentException("Error updating room messages: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, updateStmt, null);
		}
	}

	/**
	 * Updates message and transaction ids for a message pair produced from one
	 * transaction.
	 *
	 * @param transactionId original transaction/message id
	 * @param newMessageId  new persisted message id
	 * @param messageType   message type to update
	 */
	public static void updateMessageIds(String transactionId, String newMessageId, MessageType messageType) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String mType = null;
		if (messageType.equals(MessageType.INPUT_TEXT)) {
			mType = "INPUT";
		}
		if (messageType.equals(MessageType.RESPONSE_TEXT)) {
			mType = "RESPONSE";
		}
		if (mType == null) {
			// Non-persisted message types (e.g. RESPONSE_TOOL, INPUT_TOOL_EXEC) have no
			// corresponding MESSAGE table row -- silently skip rather than error.
			return;
		}

		PreparedStatement updateStmt = null;
		try {
			String updateQuery = "UPDATE MESSAGE SET MESSAGE_ID=?, TRANSACTION_ID=? WHERE MESSAGE_ID=? AND MESSAGE_TYPE=?";
			updateStmt = modelInferenceLogsDb.getPreparedStatement(updateQuery);
			updateStmt.setString(1, newMessageId);
			updateStmt.setString(2, transactionId);
			updateStmt.setString(3, transactionId);
			updateStmt.setString(4, mType);
			updateStmt.execute();
			if (!updateStmt.getConnection().getAutoCommit()) {
				updateStmt.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(
					"Failed to migrate message ids for transactionId '{}' to newMessageId '{}' for messageType '{}'.",
					transactionId, newMessageId, messageType, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, updateStmt, null);
		}
	}

	/**
	 * Updates room message history plus room metadata (name/model id).
	 *
	 * @param roomId         room identifier
	 * @param userId         user identifier
	 * @param messageHistory serialized message history payload
	 * @param roomName       new room name
	 * @param engineId       model/engine identifier
	 * @return {@code true} when at least one row is updated
	 */
	public static boolean llm2_updateRoomMessages(String roomId, String userId, String messageHistory, String roomName,
			String engineId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		PreparedStatement updateStmt = null;
		try {
			// Update messages and timestamp where room and user match
			String query = "UPDATE ROOM SET MESSAGES = ?, UPDATED_AT = ? , ROOM_NAME = ?, MODEL_ID = ?  WHERE ROOM_ID = ? AND USER_ID = ?";
			updateStmt = modelInferenceLogsDb.getPreparedStatement(query);

			// Prepare statement
			updateStmt.setString(1, messageHistory);
			updateStmt.setTimestamp(2, Utility.getCurrentSqlTimestampUTC());
			updateStmt.setString(3, roomName);
			updateStmt.setString(4, engineId);
			updateStmt.setString(5, roomId);
			updateStmt.setString(6, userId);

			// Execute update
			int rows = updateStmt.executeUpdate();
			if (!updateStmt.getConnection().getAutoCommit()) {
				updateStmt.getConnection().commit();
			}
			return rows > 0;

		} catch (Exception e) {
			classLogger.error("Failed to update room messages for roomId '{}' and userId '{}'.", roomId, userId, e);
			throw new IllegalArgumentException("Error updating room messages: " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, updateStmt, null);
		}
	}

	/**
	 * Retrieves a room entity for a user/room pair.
	 *
	 * @param roomId room identifier
	 * @param userId user identifier
	 * @return room object, or {@code null} when not found
	 */
	public static Room getRoomById(String roomId, String userId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		String query = "SELECT * FROM ROOM WHERE ROOM_ID = ? and USER_ID = ?";
		PreparedStatement stmt = null;
		ResultSet resultSet = null;
		try {
			stmt = modelInferenceLogsDb.getPreparedStatement(query);
			stmt.setString(1, roomId);
			stmt.setString(2, userId);
			resultSet = stmt.executeQuery();
			if (resultSet.next()) {
				return new Room(resultSet.getString("ROOM_ID"), resultSet.getString("USER_ID"),
						resultSet.getString("ROOM_NAME"), resultSet.getString("ROOM_CONTEXT"),
						resultSet.getString("PROJECT_ID"), resultSet.getString("SHARE_ID"),
						resultSet.getBoolean("IS_ACTIVE"), resultSet.getTimestamp("DATE_CREATED"),
						resultSet.getTimestamp("UPDATED_AT"), resultSet.getString("MESSAGES"),
						resultSet.getBoolean("PINNED"), resultSet.getString("OPTIONS"), resultSet.getString("MODEL_ID"),
						resultSet.getString("PARENT_ROOM_ID"));
			}
		} catch (SQLException e) {
			classLogger.error("Failed to retrieve room for roomId '{}' and userId '{}'.", roomId, userId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, stmt, resultSet);
		}
		return null;
	}

	/**
	 * Returns active-room rows for the specified user/room pair.
	 *
	 * @param roomId room identifier
	 * @param userId user identifier
	 * @return active-room rows
	 */
	public static List<Map<String, Object>> getUserActiveRooms(String roomId, String userId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "IS_ACTIVE"));

		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(ROOM_TABLE_NAME + "IS_ACTIVE", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ROOM_TABLE_NAME + "ROOM_ID", "==", roomId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ROOM_TABLE_NAME + "USER_ID", "==", userId));

		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Validates that a room exists for a user and is currently active.
	 *
	 * @param roomId room identifier
	 * @param userId user identifier
	 * @return {@code true} when the room is valid and active
	 */
	public static boolean validUserRoom(String roomId, String userId) {
		// check if the user has access and the room is active
		List<Map<String, Object>> roomActiveOutput = getUserActiveRooms(roomId, userId);
		// if there are no rooms or more than one returned, throw an error
		if (roomActiveOutput.size() != 1) {
			throw new IllegalArgumentException("Unable to find room");
		}
		// if it isn't active, throw an error
		if (roomActiveOutput.get(0).get("IS_ACTIVE").equals(false)) {
			throw new IllegalArgumentException("Room is closed");
		}
		return true;
	}

	/* -------- WORKSPACE PIECES ------- */

	/**
	 * Creates a workspace record and optional workspace-resource rows.
	 *
	 * @param workspaceId          workspace identifier
	 * @param ownerId              owner user id
	 * @param workspaceName        workspace display name
	 * @param workspaceDescription workspace description payload
	 * @param systemPrompt         workspace system prompt payload
	 * @param resources            optional workspace resources
	 * @throws Exception if creation fails
	 */
	public static void createNewWorkspaceEntry(String workspaceId, String ownerId, String workspaceName,
			String workspaceDescription, String systemPrompt, List<Map<String, String>> resources) throws Exception {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		Timestamp now = Utility.getCurrentSqlTimestampUTC();

		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try (PreparedStatement ps = con.prepareStatement(
					"INSERT INTO WORKSPACE (WORKSPACE_ID, NAME, DESCRIPTION, SYSTEM_PROMPT, OWNER, IS_ACTIVE, DATE_CREATED, DATE_UPDATED) VALUES (?,?,?,?,?,?,?,?)")) {
				int index = 1;
				ps.setString(index++, workspaceId);
				ps.setString(index++, workspaceName);
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(con, ps, workspaceDescription, index++, GSON);
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(con, ps, systemPrompt, index++, GSON);
				ps.setString(index++, ownerId);
				ps.setBoolean(index++, true);
				ps.setTimestamp(index++, now);
				ps.setTimestamp(index++, now);
				ps.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}

			if (resources == null || resources.isEmpty()) {
				return;
			}
			try (PreparedStatement ps = con.prepareStatement(
					"INSERT INTO WORKSPACE_RESOURCE (WORKSPACE_RESOURCE_ID, WORKSPACE_ID, RESOURCE_ID, RESOURCE_TYPE, RESOURCE_SUBTYPE) VALUES (?,?,?,?,?)")) {
				for (Map<String, String> res : resources) {
					int index = 1;
					ps.setString(index++, res.get("workspace_resource_id"));
					ps.setString(index++, res.get("workspace_id"));
					ps.setString(index++, res.get("resource_id"));
					ps.setString(index++, res.get("resource_type"));
					ps.setString(index++, res.get("resource_subtype"));
					ps.addBatch();
				}
				ps.executeBatch();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to create workspace '{}' for owner '{}' with resources.", workspaceId, ownerId,
					e);
			throw new IllegalArgumentException("Error creating workspace: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}

	/**
	 * Updates a workspace record and replaces <b>all</b> of its resource
	 * associations - MCP engines/projects, prompts, and skills alike.
	 * <p>
	 * The supplied {@code resources} list is treated as the complete desired set:
	 * every existing {@code WORKSPACE_RESOURCE} row for the workspace is deleted
	 * and replaced with the rows derived from {@code resources}. Callers must
	 * therefore pass the full resource set (including any skills to keep); a
	 * resource omitted from the list is detached. (Incremental skill attach/detach
	 * without a full rewrite is available via {@code AttachSkillToWorkspaceReactor}
	 * / {@code DetachSkillFromWorkspaceReactor}.)
	 *
	 * @param workspaceId          workspace identifier
	 * @param workspaceName        workspace display name
	 * @param workspaceDescription workspace description payload
	 * @param systemPrompt         workspace system prompt payload
	 * @param isActive             active state
	 * @param resources            complete replacement resource list (engines,
	 *                             projects, prompts, and skills)
	 * @throws Exception if update fails
	 */
	public static void updateWorkspaceEntry(String workspaceId, String workspaceName, String workspaceDescription,
			String systemPrompt, boolean isActive, List<Map<String, String>> resources) throws Exception {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		Timestamp now = Utility.getCurrentSqlTimestampUTC();

		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try (PreparedStatement ps = con.prepareStatement(
					"UPDATE WORKSPACE SET NAME = ?, DESCRIPTION = ?, SYSTEM_PROMPT = ?, IS_ACTIVE = ?, DATE_UPDATED = ? WHERE WORKSPACE_ID = ?")) {
				int index = 1;
				ps.setString(index++, workspaceName);
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(con, ps, workspaceDescription, index++, GSON);
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(con, ps, systemPrompt, index++, GSON);
				ps.setBoolean(index++, isActive);
				ps.setTimestamp(index++, now);
				ps.setString(index++, workspaceId);
				ps.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}

			try (PreparedStatement ps = con.prepareStatement("DELETE FROM WORKSPACE_RESOURCE WHERE WORKSPACE_ID = ?")) {
				int index = 1;
				ps.setString(index++, workspaceId);
				ps.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}

			if (resources == null || resources.isEmpty()) {
				return;
			}
			try (PreparedStatement ps = con.prepareStatement(
					"INSERT INTO WORKSPACE_RESOURCE (WORKSPACE_RESOURCE_ID, WORKSPACE_ID, RESOURCE_ID, RESOURCE_TYPE, RESOURCE_SUBTYPE) VALUES (?,?,?,?,?)")) {
				for (Map<String, String> res : resources) {
					int index = 1;
					ps.setString(index++, res.get("workspace_resource_id"));
					ps.setString(index++, res.get("workspace_id"));
					ps.setString(index++, res.get("resource_id"));
					ps.setString(index++, res.get("resource_type"));
					ps.setString(index++, res.get("resource_subtype"));
					ps.addBatch();
				}
				ps.executeBatch();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to update workspace '{}' and refresh workspace resources.", workspaceId, e);
			throw new IllegalArgumentException("Error updating workspace: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}

	/**
	 * Deletes a workspace and associated links to resources and rooms.
	 *
	 * @param workspaceId workspace identifier
	 */
	public static void deleteWorkspaceEntry(String workspaceId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try (PreparedStatement ps1 = con.prepareStatement("DELETE FROM WORKSPACE_RESOURCE WHERE WORKSPACE_ID = ?");
					PreparedStatement ps2 = con
							.prepareStatement("UPDATE ROOM SET WORKSPACE_ID = NULL WHERE WORKSPACE_ID = ?");
					PreparedStatement ps3 = con.prepareStatement("DELETE FROM WORKSPACE WHERE WORKSPACE_ID = ?");) {
				ps1.setString(1, workspaceId);
				ps2.setString(1, workspaceId);
				ps3.setString(1, workspaceId);
				ps1.execute();
				ps2.execute();
				ps3.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to delete workspace '{}' and related room/resource links.", workspaceId, e);
			throw new IllegalArgumentException("Error deleting workspace: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}

	/**
	 * Fetches one workspace entry by id.
	 *
	 * @param workspaceId workspace identifier
	 * @return workspace map, or {@code null} when not found
	 */
	public static Map<String, Object> getWorkspaceEntry(String workspaceId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("WORKSPACE__WORKSPACE_ID", "workspace_id"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE__DESCRIPTION", "description"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE__SYSTEM_PROMPT", "system_prompt"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE__CONFIG_JSON", "config_json"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE__OWNER", "owner"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE__IS_ACTIVE", "is_active"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE__DATE_CREATED", "date_created"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE__DATE_UPDATED", "date_updated"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("WORKSPACE__WORKSPACE_ID", "==", workspaceId));

		qs.setLimit(1L);

		Map<String, Object> result = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs)) {
			while (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				result = new HashMap<String, Object>();
				for (int i = 0; i < headers.length; i++) {
					if (values[i] instanceof java.sql.Clob) {
						String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
						result.put(headers[i], value);
					} else if (values[i] instanceof java.sql.Blob) {
						String value = AbstractSqlQueryUtil.flushBlobToString((java.sql.Blob) values[i]);
						result.put(headers[i], value);
					} else if (values[i] instanceof prerna.date.SemossDate) {
						String value = ((prerna.date.SemossDate) values[i]).getFormatted("yyyy-MM-dd'T'HH:mm:ss'Z'");
						result.put(headers[i], value);
					} else {
						result.put(headers[i], values[i]);
					}
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to fetch workspace entry for workspaceId '{}'.", workspaceId, e);
		}
		return result;
	}

	/**
	 * Returns the parsed {@code WORKSPACE.CONFIG_JSON} for a workspace, or
	 * {@code null} when the column is missing / empty / unparseable.
	 *
	 * <p>
	 * {@code CONFIG_JSON} carries the per-workspace agent config - system prompt
	 * mirror, MCPs, budgets, hooks. Consumers (AgentConfigLoader,
	 * Room.getAllToolsJsonForRoom) layer this on top of the legacy column /
	 * WORKSPACE_RESOURCE reads.
	 *
	 * @param workspaceId workspace identifier
	 * @return parsed JSON object, or {@code null}
	 */
	public static JSONObject getWorkspaceConfigJson(String workspaceId) {
		if (workspaceId == null) {
			return null;
		}
		Map<String, Object> row = getWorkspaceEntry(workspaceId);
		if (row == null) {
			return null;
		}
		Object raw = row.get("config_json");
		if (raw == null) {
			return null;
		}
		String text = String.valueOf(raw).trim();
		if (text.isEmpty()) {
			return null;
		}
		try {
			return new JSONObject(text);
		} catch (Exception e) {
			classLogger.warn("Failed to parse WORKSPACE.CONFIG_JSON for workspaceId '{}': {}", workspaceId,
					e.getMessage());
			return null;
		}
	}

	/**
	 * Writes {@code WORKSPACE.CONFIG_JSON} for a workspace. Pass {@code null} to
	 * clear the column.
	 *
	 * @param workspaceId workspace identifier
	 * @param configJson  parsed JSON to persist, or {@code null} to clear
	 * @throws SQLException if the update fails
	 */
	public static void updateWorkspaceConfigJson(String workspaceId, JSONObject configJson) throws SQLException {
		if (workspaceId == null || workspaceId.isEmpty()) {
			throw new IllegalArgumentException("workspaceId is required");
		}
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		Timestamp now = Utility.getCurrentSqlTimestampUTC();
		String serialized = configJson == null ? null : configJson.toString();

		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try (PreparedStatement ps = con.prepareStatement(
					"UPDATE WORKSPACE SET CONFIG_JSON = ?, DATE_UPDATED = ? WHERE WORKSPACE_ID = ?")) {
				int index = 1;
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(con, ps, serialized, index++, GSON);
				ps.setTimestamp(index++, now);
				ps.setString(index++, workspaceId);
				ps.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to update CONFIG_JSON for workspaceId '{}'.", workspaceId, e);
			throw new SQLException("Failed to update workspace CONFIG_JSON: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}

	/**
	 * Updates only the core {@code WORKSPACE} display columns (NAME, DESCRIPTION,
	 * SYSTEM_PROMPT) plus DATE_UPDATED. Leaves IS_ACTIVE, CONFIG_JSON, and the
	 * WORKSPACE_RESOURCE rows untouched - unlike
	 * {@link #updateWorkspaceEntry(String, String, String, String, boolean, List)},
	 * which replaces the full resource set.
	 *
	 * <p>
	 * Used by {@code SystemAgentSeeder} to self-heal the legacy display columns on
	 * every boot the same way {@link #updateWorkspaceConfigJson(String, JSONObject)}
	 * self-heals the config mirror. The legacy SYSTEM_PROMPT column is what
	 * GetWorkspace/ListWorkspaces surface to the FE, so it must track the seeded
	 * prompt or the UI shows a stale value after the constant changes.
	 *
	 * @param workspaceId  workspace identifier
	 * @param name         workspace display name
	 * @param description  workspace description payload
	 * @param systemPrompt workspace system prompt payload
	 * @throws SQLException if the update fails
	 */
	public static void updateWorkspaceCoreFields(String workspaceId, String name, String description,
			String systemPrompt) throws SQLException {
		if (workspaceId == null || workspaceId.isEmpty()) {
			throw new IllegalArgumentException("workspaceId is required");
		}
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		Timestamp now = Utility.getCurrentSqlTimestampUTC();

		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try (PreparedStatement ps = con.prepareStatement(
					"UPDATE WORKSPACE SET NAME = ?, DESCRIPTION = ?, SYSTEM_PROMPT = ?, DATE_UPDATED = ? WHERE WORKSPACE_ID = ?")) {
				int index = 1;
				ps.setString(index++, name);
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(con, ps, description, index++, GSON);
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(con, ps, systemPrompt, index++, GSON);
				ps.setTimestamp(index++, now);
				ps.setString(index++, workspaceId);
				ps.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to update core fields for workspaceId '{}'.", workspaceId, e);
			throw new SQLException("Failed to update workspace core fields: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}

	/**
	 * Adds a skill reference to {@code WORKSPACE.CONFIG_JSON.skills[]}, mirroring
	 * the authoritative {@code WORKSPACE_RESOURCE} row so the two stores stay in
	 * sync - the same dual-write pattern MCPs use
	 * ({@code EditWorkspaceReactor.mirrorCoreFieldsIntoConfigJson}). Idempotent: a
	 * skill already present (matched by {@code skill_id}) is left untouched and no
	 * write is issued.
	 *
	 * <p>
	 * The entry shape matches what {@code AgentConfigLoader.resolveSkills} reads:
	 * {@code { "skill_id": <id>, "pinned_version": <optional> }}. Note the key is
	 * {@code skill_id} - NOT {@code id} as the MCP mirror uses.
	 *
	 * @param workspaceId   workspace identifier
	 * @param skillId       skill identifier to add
	 * @param pinnedVersion optional pinned version; the key is omitted when
	 *                      null/empty
	 * @throws SQLException if the CONFIG_JSON write fails
	 */
	public static void addSkillToWorkspaceConfigJson(String workspaceId, String skillId, String pinnedVersion)
			throws SQLException {
		if (workspaceId == null || workspaceId.isEmpty()) {
			throw new IllegalArgumentException("workspaceId is required");
		}
		if (skillId == null || skillId.isEmpty()) {
			throw new IllegalArgumentException("skillId is required");
		}
		JSONObject cfg = getWorkspaceConfigJson(workspaceId);
		if (cfg == null) {
			cfg = new JSONObject();
			cfg.put("schema_version", 1);
		}
		JSONArray skills = cfg.optJSONArray("skills");
		if (skills == null) {
			skills = new JSONArray();
		}
		// Dedup by skill_id - keep the mirror idempotent like the attach reactor
		// itself.
		for (int i = 0; i < skills.length(); i++) {
			JSONObject s = skills.optJSONObject(i);
			if (s != null && skillId.equals(s.optString("skill_id", null))) {
				return;
			}
		}
		JSONObject entry = new JSONObject();
		entry.put("skill_id", skillId);
		if (pinnedVersion != null && !pinnedVersion.isEmpty()) {
			entry.put("pinned_version", pinnedVersion);
		}
		skills.put(entry);
		cfg.put("skills", skills);
		updateWorkspaceConfigJson(workspaceId, cfg);
	}

	/**
	 * Removes a skill reference from {@code WORKSPACE.CONFIG_JSON.skills[]},
	 * mirroring deletion of the {@code WORKSPACE_RESOURCE} row. No-op (no write)
	 * when the workspace has no CONFIG_JSON, no {@code skills} array, or the skill
	 * is not present.
	 *
	 * @param workspaceId workspace identifier
	 * @param skillId     skill identifier to remove
	 * @throws SQLException if the CONFIG_JSON write fails
	 */
	public static void removeSkillFromWorkspaceConfigJson(String workspaceId, String skillId) throws SQLException {
		if (workspaceId == null || workspaceId.isEmpty()) {
			throw new IllegalArgumentException("workspaceId is required");
		}
		if (skillId == null || skillId.isEmpty()) {
			throw new IllegalArgumentException("skillId is required");
		}
		JSONObject cfg = getWorkspaceConfigJson(workspaceId);
		if (cfg == null) {
			return;
		}
		JSONArray skills = cfg.optJSONArray("skills");
		if (skills == null || skills.length() == 0) {
			return;
		}
		JSONArray kept = new JSONArray();
		boolean removed = false;
		for (int i = 0; i < skills.length(); i++) {
			JSONObject s = skills.optJSONObject(i);
			if (s != null && skillId.equals(s.optString("skill_id", null))) {
				removed = true;
				continue;
			}
			kept.put(skills.get(i));
		}
		if (!removed) {
			return;
		}
		cfg.put("skills", kept);
		updateWorkspaceConfigJson(workspaceId, cfg);
	}

	/**
	 * Returns paginated room entries for a workspace visible to the requesting
	 * user.
	 *
	 * @param workspaceId workspace identifier
	 * @param user        requesting user
	 * @param limit       max rows to return
	 * @param offset      rows to skip
	 * @param filters     optional additional filters
	 * @param sorts       optional sort overrides
	 * @return paginated room payload with total count
	 */
	public static Map<String, Object> getWorkspaceRoomsForUser(String workspaceId, User user, long limit, long offset,
			GenRowFilters filters, List<IQuerySort> sorts) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		Collection<String> userIds = getUserFiltersQs(user);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ROOM__ROOM_ID", "room_id"));
		qs.addSelector(new QueryColumnSelector("ROOM__ROOM_NAME", "room_name"));
		qs.addSelector(new QueryColumnSelector("ROOM__AGENT_ID", "model_id"));
		qs.addSelector(new QueryColumnSelector("ROOM__WORKSPACE_ID", "workspace_id"));
		qs.addSelector(new QueryColumnSelector("ROOM__DATE_CREATED", "date_created"));
		qs.addSelector(new QueryColumnSelector("ROOM__UPDATED_AT", "date_updated"));
		qs.addSelector(new QueryOpaqueSelector("COUNT(*) OVER()", "total_row_count"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__WORKSPACE_ID", "==", workspaceId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userIds));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ROOM__IS_ACTIVE", "==", true, PixelDataType.BOOLEAN));

		// room has at least one non-null message
		SelectQueryStruct messageExistsQs = new SelectQueryStruct();
		messageExistsQs.addSelector(new QueryColumnSelector("MESSAGE__ROOM_ID"));
		messageExistsQs.addRelation("MESSAGE__ROOM_ID", "ROOM__ROOM_ID", "inner.join");
		messageExistsQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_DATA", "!=", null));
		messageExistsQs
				.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__WORKSPACE_ID", "==", workspaceId));
		messageExistsQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userIds));
		messageExistsQs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ROOM__IS_ACTIVE", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ROOM__ROOM_ID", "==", messageExistsQs));

		// append other filters directly in
		if (filters != null && !filters.isEmpty()) {
			qs.mergeExplicitFilters(filters);
		}

		qs.setLimit(limit);
		qs.setOffSet(offset);
		if (sorts == null || sorts.isEmpty()) {
			qs.addOrderBy("ROOM__DATE_CREATED", "DESC");
		} else {
			qs.addOrderBy(sorts);
		}

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs)) {
			Map<String, Object> workspaces = new HashMap<>();
			List<Map<String, Object>> roomDetails = new ArrayList<>();
			Long totalCount = 0L;
			while (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 0; i < headers.length; i++) {
					if (values[i] instanceof java.sql.Clob) {
						String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
						map.put(headers[i], value);
					} else if (values[i] instanceof java.sql.Blob) {
						String value = AbstractSqlQueryUtil.flushBlobToString((java.sql.Blob) values[i]);
						map.put(headers[i], value);
					} else if (values[i] instanceof prerna.date.SemossDate) {
						String value = ((prerna.date.SemossDate) values[i]).getFormatted("yyyy-MM-dd'T'HH:mm:ss'Z'");
						map.put(headers[i], value);
					} else {
						map.put(headers[i], values[i]);
					}
				}

				Object totalCountObj = map.remove("total_row_count");
				if (totalCountObj != null && totalCount == 0) {
					if (totalCountObj instanceof Number) {
						totalCount = ((Number) totalCountObj).longValue();
					} else {
						classLogger.warn("Unexpected total_row_count type: {}", totalCountObj.getClass());
					}
				}
				roomDetails.add(map);
			}
			workspaces.put("total_count", totalCount);
			workspaces.put("rooms", roomDetails);
			return workspaces;
		} catch (Exception e) {
			classLogger.error("Failed to fetch workspace rooms for workspaceId '{}'.", workspaceId, e);
			return null;
		}
	}

	/**
	 * Returns paginated workspace entries visible to the user.
	 *
	 * @param user               requesting user
	 * @param limit              max rows to return
	 * @param offset             rows to skip
	 * @param filters            optional additional filters
	 * @param sorts              optional sort overrides
	 * @param sharedWorkspaceIds workspace ids visible to the user
	 * @return paginated workspace payload with total count
	 */
	public static Map<String, Object> getWorkspaceEntriesForUser(User user, long limit, long offset,
			GenRowFilters filters, List<IQuerySort> sorts, Set<String> sharedWorkspaceIds) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();

		// This can get reworked but only does anything if sharedWorkspaceIds is not
		// null
		if (sharedWorkspaceIds == null || sharedWorkspaceIds.isEmpty()) {
			return new HashMap<String, Object>();
		}

		Collection<String> userIds = getUserFiltersQs(user);

		SelectQueryStruct subQs = new SelectQueryStruct();
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__WORKSPACE_ID", "workspace_id"));
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__NAME", "name"));
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__DESCRIPTION", "description"));
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__SYSTEM_PROMPT", "system_prompt"));
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__OWNER", "owner"));
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__IS_ACTIVE", "is_active"));
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__DATE_CREATED", "date_created"));
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__DATE_UPDATED", "date_updated"));

		subQs.addSelector(QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter("WORKSPACE__OWNER", "==", userIds),
				new QueryConstantSelector(Boolean.TRUE), new QueryConstantSelector(Boolean.FALSE), "is_creator"));

		subQs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("WORKSPACE__WORKSPACE_ID", "==", sharedWorkspaceIds));

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryTypedColumnSelector("subquery__workspace_id", "workspace_id", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__name", "name", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__description", "description", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__system_prompt", "system_prompt", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__owner", "owner", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__is_active", "is_active", SemossDataType.BOOLEAN));
		qs.addSelector(new QueryTypedColumnSelector("subquery__date_created", "date_created", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__date_updated", "date_updated", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__is_creator", "is_creator", SemossDataType.BOOLEAN));
		qs.addSelector(new QueryOpaqueSelector("COUNT(*) OVER()", "total_row_count"));

		if (filters != null && !filters.isEmpty()) {
			qs.mergeExplicitFilters(filters);
		}

		qs.setLimit(limit);
		qs.setOffSet(offset);

		if (sorts == null || sorts.isEmpty()) {
			qs.addOrderBy("date_updated", "DESC");
		} else {
			qs.addOrderBy(sorts);
		}

		IQueryInterpreter interpreter = modelInferenceLogsDb.getQueryInterpreter();
		interpreter.setQueryStruct(subQs);
		String subQuery = interpreter.composeQuery();
		qs.setCustomFrom(subQuery);
		qs.setCustomFromAliasName("subquery");

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs)) {
			Map<String, Object> workspaces = new HashMap<>();
			List<Map<String, Object>> workspaceDetails = new ArrayList<>();
			Long totalCount = 0L;
			while (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 0; i < headers.length; i++) {
					if (values[i] instanceof java.sql.Clob) {
						String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
						map.put(headers[i], value);
					} else if (values[i] instanceof java.sql.Blob) {
						String value = AbstractSqlQueryUtil.flushBlobToString((java.sql.Blob) values[i]);
						map.put(headers[i], value);
					} else if (values[i] instanceof prerna.date.SemossDate) {
						String value = ((prerna.date.SemossDate) values[i]).getFormatted("yyyy-MM-dd'T'HH:mm:ss'Z'");
						map.put(headers[i], value);
					} else {
						map.put(headers[i], values[i]);
					}
				}
				Object totalCountObj = map.remove("total_row_count");
				if (totalCount == 0 && totalCountObj != null) {
					if (totalCountObj instanceof Number) {
						totalCount = ((Number) totalCountObj).longValue();
					} else {
						classLogger.warn("Unexpected total_row_count type: {}", totalCountObj.getClass());
					}
				}
				workspaceDetails.add(map);
			}
			workspaces.put("total_count", totalCount);
			workspaces.put("workspaces", workspaceDetails);
			return workspaces;
		} catch (Exception e) {
			classLogger.error("Failed to fetch workspace entries for user-visible workspace ids.", e);
			return null;
		}
	}

	/**
	 * Builds sanitized user-id filter values for all login providers on the user.
	 *
	 * @param user user whose login ids should be extracted
	 * @return sanitized user id collection
	 */
	private static Collection<String> getUserFiltersQs(User user) {
		List<String> filters = new ArrayList<String>();
		if (user != null) {
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider thisLogin : logins) {
				filters.add(Utility.inputSQLSanitizer(user.getAccessToken(thisLogin).getId()));
			}
		}

		return filters;
	}

	/**
	 * Fetches workspace-resource rows by workspace and optional resource types.
	 *
	 * @param workspaceId   workspace identifier
	 * @param resourceTypes resource types filter (nullable)
	 * @return workspace resource rows
	 */
	public static List<Map<String, Object>> getWorkspaceResourcesByType(String workspaceId,
			List<String> resourceTypes) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("WORKSPACE_RESOURCE__WORKSPACE_RESOURCE_ID", "workspace_resource_id"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE_RESOURCE__WORKSPACE_ID", "workspace_id"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE_RESOURCE__RESOURCE_ID", "resource_id"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE_RESOURCE__RESOURCE_TYPE", "resource_type"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE_RESOURCE__RESOURCE_SUBTYPE", "resource_subtype"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("WORKSPACE_RESOURCE__WORKSPACE_ID", "==", workspaceId));
		if (resourceTypes != null && resourceTypes.isEmpty()) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("WORKSPACE_RESOURCE__RESOURCE_TYPE", "==", resourceTypes));
		}

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs)) {
			List<Map<String, Object>> results = new ArrayList<>();
			while (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 0; i < headers.length; i++) {
					if (values[i] instanceof java.sql.Clob) {
						String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
						map.put(headers[i], value);
					} else if (values[i] instanceof java.sql.Blob) {
						String value = AbstractSqlQueryUtil.flushBlobToString((java.sql.Blob) values[i]);
						map.put(headers[i], value);
					} else {
						map.put(headers[i], values[i]);
					}
				}
				results.add(map);
			}
			return results;
		} catch (Exception e) {
			classLogger.error("Failed to fetch workspace resources for workspaceId '{}' and resourceTypes '{}'.",
					workspaceId, resourceTypes, e);
			return null;
		}
	}

	/**
	 * Fetches workspace-resource rows by workspace and optional ignoring resource
	 * types.
	 *
	 * @param workspaceId   workspace identifier
	 * @param resourceTypes resource types filter to ignore (nullable)
	 * @return workspace resource rows
	 */
	public static List<Map<String, Object>> getWorkspaceResourcesIgnoringType(String workspaceId,
			List<String> resourceTypes) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("WORKSPACE_RESOURCE__WORKSPACE_RESOURCE_ID", "workspace_resource_id"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE_RESOURCE__WORKSPACE_ID", "workspace_id"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE_RESOURCE__RESOURCE_ID", "resource_id"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE_RESOURCE__RESOURCE_TYPE", "resource_type"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE_RESOURCE__RESOURCE_SUBTYPE", "resource_subtype"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("WORKSPACE_RESOURCE__WORKSPACE_ID", "==", workspaceId));
		if (resourceTypes != null && !resourceTypes.isEmpty()) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("WORKSPACE_RESOURCE__RESOURCE_TYPE", "!=", resourceTypes));
		}

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs)) {
			List<Map<String, Object>> results = new ArrayList<>();
			while (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 0; i < headers.length; i++) {
					if (values[i] instanceof java.sql.Clob) {
						String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
						map.put(headers[i], value);
					} else if (values[i] instanceof java.sql.Blob) {
						String value = AbstractSqlQueryUtil.flushBlobToString((java.sql.Blob) values[i]);
						map.put(headers[i], value);
					} else {
						map.put(headers[i], values[i]);
					}
				}
				results.add(map);
			}
			return results;
		} catch (Exception e) {
			classLogger.error("Failed to fetch workspace resources for workspaceId '{}' and resourceTypes '{}'.",
					workspaceId, resourceTypes, e);
			return null;
		}
	}

	/**
	 * Creates one workspace-resource association row.
	 *
	 * @param workspaceResourceId workspace-resource identifier
	 * @param workspaceId         workspace identifier
	 * @param resourceId          resource identifier
	 * @param resourceType        resource type
	 * @param resourceSubType     resource subtype
	 * @throws Exception if insert fails
	 */
	public static void createNewWorkspaceResource(String workspaceResourceId, String workspaceId, String resourceId,
			String resourceType, String resourceSubType) throws Exception {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try (PreparedStatement ps = con.prepareStatement(
					"INSERT INTO WORKSPACE_RESOURCE (WORKSPACE_RESOURCE_ID, WORKSPACE_ID, RESOURCE_ID, RESOURCE_TYPE, RESOURCE_SUBTYPE) VALUES (?,?,?,?,?)")) {
				int index = 1;
				ps.setString(index++, workspaceResourceId);
				ps.setString(index++, workspaceId);
				ps.setString(index++, resourceId);
				ps.setString(index++, resourceType);
				ps.setString(index++, resourceSubType);
				ps.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to create workspace resource '{}' for workspaceId '{}'.", workspaceResourceId,
					workspaceId, e);
			throw new IllegalArgumentException("Error creating workspace resource: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}

	/**
	 * Returns the workspace-resource row matching the (workspaceId, resourceId,
	 * resourceType) tuple, or {@code null} when no row exists. Used by the skill
	 * attach reactor to detect already-attached skills before issuing an insert.
	 *
	 * @param workspaceId  workspace identifier
	 * @param resourceId   resource identifier (e.g. SKILL_ID)
	 * @param resourceType resource type discriminator (e.g. "SKILL")
	 * @return row with keys {@code workspace_resource_id, workspace_id,
	 *         resource_id, resource_type, resource_subtype}, or {@code null}
	 */
	public static Map<String, Object> findWorkspaceResource(String workspaceId, String resourceId,
			String resourceType) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try (PreparedStatement ps = con.prepareStatement(
					"SELECT WORKSPACE_RESOURCE_ID, WORKSPACE_ID, RESOURCE_ID, RESOURCE_TYPE, RESOURCE_SUBTYPE "
							+ "FROM WORKSPACE_RESOURCE WHERE WORKSPACE_ID = ? AND RESOURCE_ID = ? AND RESOURCE_TYPE = ?")) {
				ps.setString(1, workspaceId);
				ps.setString(2, resourceId);
				ps.setString(3, resourceType);
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						Map<String, Object> row = new HashMap<>();
						row.put("workspace_resource_id", rs.getString(1));
						row.put("workspace_id", rs.getString(2));
						row.put("resource_id", rs.getString(3));
						row.put("resource_type", rs.getString(4));
						row.put("resource_subtype", rs.getString(5));
						return row;
					}
				}
			}
		} catch (Exception e) {
			classLogger.error(
					"Failed to look up workspace resource for workspaceId '{}', resourceId '{}', resourceType '{}'.",
					workspaceId, resourceId, resourceType, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
		return null;
	}

	/**
	 * Deletes the workspace-resource row(s) matching the (workspaceId, resourceId,
	 * resourceType) tuple. No-op when no row exists.
	 *
	 * @param workspaceId  workspace identifier
	 * @param resourceId   resource identifier (e.g. SKILL_ID)
	 * @param resourceType resource type discriminator (e.g. "SKILL")
	 * @return number of rows deleted
	 */
	public static int deleteWorkspaceResource(String workspaceId, String resourceId, String resourceType) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try (PreparedStatement ps = con.prepareStatement(
					"DELETE FROM WORKSPACE_RESOURCE WHERE WORKSPACE_ID = ? AND RESOURCE_ID = ? AND RESOURCE_TYPE = ?")) {
				ps.setString(1, workspaceId);
				ps.setString(2, resourceId);
				ps.setString(3, resourceType);
				int deleted = ps.executeUpdate();
				if (!con.getAutoCommit()) {
					con.commit();
				}
				return deleted;
			}
		} catch (Exception e) {
			classLogger.error(
					"Failed to delete workspace resource for workspaceId '{}', resourceId '{}', resourceType '{}'.",
					workspaceId, resourceId, resourceType, e);
			throw new IllegalArgumentException("Error deleting workspace resource: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}

	/**
	 * Fetches workspace resources with optional type/subtype filters.
	 *
	 * @param workspaceId     workspace identifier
	 * @param resourceType    resource type filter (nullable)
	 * @param resourceSubType resource subtype filter (nullable)
	 * @return workspace resource rows
	 */
	public static List<Map<String, String>> getWorkspaceResources(String workspaceId, String resourceType,
			String resourceSubType) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		Connection con = null;
		List<Map<String, String>> resources = new ArrayList<>();
		try {
			con = modelInferenceLogsDb.getConnection();
			// Build base query
			StringBuilder sql = new StringBuilder("SELECT RESOURCE_ID, RESOURCE_TYPE, RESOURCE_SUBTYPE "
					+ "FROM WORKSPACE_RESOURCE WHERE WORKSPACE_ID = ?");
			List<Object> params = new ArrayList<>();
			params.add(workspaceId);

			if (resourceType != null) {
				sql.append(" AND RESOURCE_TYPE = ?");
				params.add(resourceType);
			}
			if (resourceSubType != null) {
				sql.append(" AND RESOURCE_SUBTYPE = ?");
				params.add(resourceSubType);
			}

			try (PreparedStatement ps = con.prepareStatement(sql.toString())) {
				for (int i = 0; i < params.size(); i++) {
					ps.setObject(i + 1, params.get(i));
				}
				ResultSet rs = ps.executeQuery();
				while (rs.next()) {
					Map<String, String> resource = new HashMap<>();
					resource.put("resource_id", rs.getString("RESOURCE_ID"));
					resource.put("resource_type", rs.getString("RESOURCE_TYPE"));
					resource.put("resource_subtype", rs.getString("RESOURCE_SUBTYPE"));
					resources.add(resource);
				}
			}
		} catch (SQLException e) {
			classLogger.error(
					"Failed to fetch workspace resources for workspaceId '{}', resourceType '{}', resourceSubType '{}'.",
					workspaceId, resourceType, resourceSubType, e);
			throw new IllegalArgumentException("Error fetching workspace resources: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
		return resources;
	}

	/**
	 * Marks a workspace as inactive.
	 *
	 * @param workspaceId workspace identifier
	 */
	public static void doSetWorkspaceToInactive(String workspaceId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try (PreparedStatement ps = con
					.prepareStatement("UPDATE WORKSPACE SET IS_ACTIVE = ? WHERE WORKSPACE_ID = ?");) {
				ps.setBoolean(1, false);
				ps.setString(2, workspaceId);
				ps.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to set workspace inactive for workspaceId '{}'.", workspaceId, e);
			throw new IllegalArgumentException("Error deactivating workspace: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}

	/**
	 * Marks a workspace as active.
	 *
	 * @param workspaceId workspace identifier
	 */
	public static void doSetWorksapceToActive(String workspaceId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try (PreparedStatement ps = con
					.prepareStatement("UPDATE WORKSPACE SET IS_ACTIVE = ? WHERE WORKSPACE_ID = ?");) {
				ps.setBoolean(1, true);
				ps.setString(2, workspaceId);
				ps.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to set workspace active for workspaceId '{}'.", workspaceId, e);
			throw new IllegalArgumentException("Error deactivating workspace: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}

	/**
	 * Returns per-user model usage aggregates for an engine.
	 *
	 * @param agentId   engine identifier
	 * @param startDate optional inclusive start date (nullable)
	 * @param endDate   optional inclusive end date (nullable)
	 * @return per-user usage rows
	 */
	public static List<Map<String, Object>> getModelInferenceUserReport(String agentId, String startDate,
			String endDate) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();

		// SELECT fields
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_NAME", "user"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_ID", "user_id"));

		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT,
				MESSAGE_TABLE_NAME + "MESSAGE_ID", "messages"));

		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM,
				MESSAGE_TABLE_NAME + "MESSAGE_TOKENS", "tokens"));

		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.AVERAGE_2,
				MESSAGE_TABLE_NAME + "MESSAGE_TOKENS", "avg_tokens"));

		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX,
				MESSAGE_TABLE_NAME + "DATE_CREATED", "last_utilized_date"));

		// WHERE AGENT_ID = ? and date filter
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "AGENT_ID", "==", agentId));

		if (startDate != null && endDate != null) {
			addStartDateEndDateFitler(qs, startDate, endDate);
		}

		// GROUP BY userName and userId
		qs.addGroupBy(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_NAME"));
		qs.addGroupBy(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_ID"));
		qs.addOrderBy("last_utilized_date", "DESC");

		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Returns per-project model usage aggregates for an engine.
	 *
	 * @param agentId   engine identifier
	 * @param startDate optional inclusive start date (nullable)
	 * @param endDate   optional inclusive end date (nullable)
	 * @return per-project usage rows
	 */
	public static List<Map<String, Object>> getModelInferenceAppReport(String agentId, String startDate,
			String endDate) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		// SELECT fields
		qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_NAME", "project_name"));
		qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_ID", "project_id"));

		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT,
				MESSAGE_TABLE_NAME + "MESSAGE_ID", "messages"));

		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM,
				MESSAGE_TABLE_NAME + "MESSAGE_TOKENS", "tokens"));

		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.AVERAGE_2,
				MESSAGE_TABLE_NAME + "MESSAGE_TOKENS", "avg_tokens"));

		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX,
				MESSAGE_TABLE_NAME + "DATE_CREATED", "last_utilized_date"));

		// JOIN MESSAGE.ROOM_ID = ROOM.ROOM_ID
		qs.addRelation(MESSAGE_TABLE_NAME + "ROOM_ID", ROOM_TABLE_NAME + "ROOM_ID", "left.join");

		// Filter on AGENT_ID
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "AGENT_ID", "==", agentId));

		if (startDate != null && endDate != null) {
			addStartDateEndDateFitler(qs, startDate, endDate);
		}

		// GROUP BY project name + id
		qs.addGroupBy(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_NAME"));
		qs.addGroupBy(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_ID"));
		qs.addOrderBy("last_utilized_date", "DESC");

		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Get user model usage per specific engines
	 *
	 * @param user      The user to get usage for
	 * @param engineIds List of engine IDs to filter by
	 * @param startDate Optional start date (format: YYYY-MM-DD)
	 * @param endDate   Optional end date (format: YYYY-MM-DD)
	 * @return List of maps containing usage data per engine
	 */
	public static List<Map<String, Object>> getUserModelUsagePerEngine(User user, List<String> engineIds,
			String startDate, String endDate) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();

		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "AGENT_ID", "ENGINE_ID"));

		// INPUT_TOKENS / RESPONSE_TOKENS: derived from MESSAGE_TOKENS via CASE so
		// that
		// records written before per-type granular columns existed are still included.
		QueryIfSelector inputIf = QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "MESSAGE_TYPE", "==", "INPUT"),
				new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_TOKENS"), new QueryConstantSelector(0),
				"INPUT_IF");
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM, inputIf, "INPUT_TOKENS"));

		QueryIfSelector responseIf = QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "MESSAGE_TYPE", "==", "RESPONSE"),
				new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_TOKENS"), new QueryConstantSelector(0),
				"RESPONSE_IF");
		qs.addSelector(
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM, responseIf, "RESPONSE_TOKENS"));

		// TOTAL_TOKENS: sum across all rows covers the full history.
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM,
				MESSAGE_TABLE_NAME + "MESSAGE_TOKENS", "TOTAL_TOKENS"));

		// Granular token detail columns: only populated for records written after
		// per-type tracking was introduced. Prefixed DETAIL_ so the reactor can nest
		// them into a TOKEN_DETAIL sub-object without conflicting with the legacy
		// names.
		qs.addSelector(
				QueryFunctionSelector.makeCoalesceSelector(
						QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM,
								MESSAGE_TABLE_NAME + "INPUT_TOKENS", null),
						new QueryConstantSelector(0), "DETAIL_INPUT_TOKENS"));
		qs.addSelector(QueryFunctionSelector.makeCoalesceSelector(
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM,
						MESSAGE_TABLE_NAME + "OUTPUT_TOKENS", null),
				new QueryConstantSelector(0), "DETAIL_OUTPUT_TOKENS"));
		qs.addSelector(QueryFunctionSelector.makeCoalesceSelector(
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM,
						MESSAGE_TABLE_NAME + "CACHE_READ_TOKENS", null),
				new QueryConstantSelector(0), "DETAIL_CACHE_READ_TOKENS"));
		qs.addSelector(QueryFunctionSelector.makeCoalesceSelector(
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM,
						MESSAGE_TABLE_NAME + "CACHE_CREATION_TOKENS", null),
				new QueryConstantSelector(0), "DETAIL_CACHE_CREATION_TOKENS"));
		qs.addSelector(QueryFunctionSelector.makeCoalesceSelector(
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM,
						MESSAGE_TABLE_NAME + "THINKING_TOKENS", null),
				new QueryConstantSelector(0), "DETAIL_THINKING_TOKENS"));

		// Count number of requests (INPUT messages only)
		QueryIfSelector requestIf = QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "MESSAGE_TYPE", "==", "INPUT"),
				new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_ID"), new QueryConstantSelector(null),
				"REQUEST_IF");
		qs.addSelector(
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT, requestIf, "TOTAL_REQUESTS"));

		// Filter by user ID
		String userId = user.getPrimaryLoginToken().getId();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "USER_ID", "==", userId));

		// Filter by engine IDs
		if (engineIds != null && !engineIds.isEmpty()) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "AGENT_ID", "==", engineIds));
		}

		// Filter by date range if provided
		addStartDateEndDateFitler(qs, startDate, endDate);

		// Group by engine
		qs.addGroupBy(new QueryColumnSelector(MESSAGE_TABLE_NAME + "AGENT_ID"));
		// Order by engine ID
		qs.addOrderBy(MESSAGE_TABLE_NAME + "AGENT_ID", "ASC");

		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Returns feedback records for the admin dashboard, joining FEEDBACK with
	 * MESSAGE and ROOM to expose user, project, and agent context.
	 *
	 * @param limit     max rows to return (nullable)
	 * @param offset    rows to skip (nullable)
	 * @param startDate inclusive lower bound on FEEDBACK_DATE (nullable)
	 * @param endDate   inclusive upper bound on FEEDBACK_DATE (nullable)
	 * @param projectId filter by project (nullable)
	 * @param userId    filter by user (nullable)
	 * @param engineId  filter by agent/engine (nullable)
	 * @return list of feedback record maps
	 */
	public static List<Map<String, Object>> getFeedbackForAdmin(String limit, String offset, String startDate,
			String endDate, String projectId, String userId, String engineId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();

		// Feedback columns
		qs.addSelector(new QueryColumnSelector(FEEDBACK_TABLE_NAME + "MESSAGE_ID"));
		qs.addSelector(new QueryColumnSelector(FEEDBACK_TABLE_NAME + "MESSAGE_TYPE"));
		qs.addSelector(new QueryColumnSelector(FEEDBACK_TABLE_NAME + "FEEDBACK_TEXT"));
		qs.addSelector(new QueryColumnSelector(FEEDBACK_TABLE_NAME + "FEEDBACK_DATE"));
		qs.addSelector(new QueryColumnSelector(FEEDBACK_TABLE_NAME + "RATING"));

		// Message columns for context
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_ID"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_NAME"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "AGENT_ID"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "DATE_CREATED"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_DATA"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "TRANSACTION_ID"));

		// Room columns for project context
		qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_ID"));
		qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_NAME"));
		qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "WORKSPACE_ID"));

		// Join FEEDBACK -> MESSAGE on MESSAGE_ID
		qs.addRelation(FEEDBACK_TABLE_NAME + "MESSAGE_ID", MESSAGE_TABLE_NAME + "MESSAGE_ID", "inner.join");
		// Join MESSAGE -> ROOM on ROOM_ID
		qs.addRelation(MESSAGE_TABLE_NAME + "ROOM_ID", ROOM_TABLE_NAME + "ROOM_ID", "left.join");

		// Optional filters
		if (projectId != null && !projectId.trim().isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ROOM_TABLE_NAME + "PROJECT_ID", "==", projectId));
		}
		if (userId != null && !userId.trim().isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "USER_ID", "==", userId));
		}
		if (engineId != null && !engineId.trim().isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "AGENT_ID", "==", engineId));
		}

		// Date range filter on FEEDBACK_DATE
		addFeedbackDateFilter(qs, startDate, endDate);

		// Default sort by FEEDBACK_DATE descending
		qs.addOrderBy(FEEDBACK_TABLE_NAME + "FEEDBACK_DATE", "DESC");

		addLimitAndOffSet(qs, limit, offset);
		List<Map<String, Object>> feedbackList = QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);

		// Ensure WORKSPACE_ID is always present in the payload (serializer drops nulls)
		for (Map<String, Object> row : feedbackList) {
			if (row.get("WORKSPACE_ID") == null) {
				row.put("WORKSPACE_ID", "");
			}
		}

		attachInputPromptsByTransactionId(modelInferenceLogsDb, feedbackList);
		return feedbackList;
	}

	/**
	 * For each feedback row in {@code feedbackList}, looks up the paired INPUT
	 * message (same TRANSACTION_ID) and stamps its MESSAGE_DATA onto the row under
	 * the {@code PROMPT} key. Rows without a matching INPUT row get a null PROMPT.
	 */
	private static void attachInputPromptsByTransactionId(IRDBMSEngine modelInferenceLogsDb,
			List<Map<String, Object>> feedbackList) {
		if (feedbackList == null || feedbackList.isEmpty()) {
			return;
		}

		Set<String> transactionIds = new HashSet<>();
		for (Map<String, Object> row : feedbackList) {
			Object txId = row.get("TRANSACTION_ID");
			if (txId != null) {
				transactionIds.add(txId.toString());
			}
		}
		if (transactionIds.isEmpty()) {
			for (Map<String, Object> row : feedbackList) {
				row.put("PROMPT", null);
			}
			return;
		}

		SelectQueryStruct inputQs = new SelectQueryStruct();
		inputQs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "TRANSACTION_ID"));
		inputQs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_DATA"));
		inputQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "TRANSACTION_ID", "==",
				new ArrayList<>(transactionIds)));
		inputQs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "MESSAGE_TYPE", "==", "INPUT"));

		List<Map<String, Object>> inputRows = QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, inputQs);
		Map<String, Object> txIdToInputData = new HashMap<>();
		for (Map<String, Object> inputRow : inputRows) {
			Object txId = inputRow.get("TRANSACTION_ID");
			if (txId != null) {
				txIdToInputData.put(txId.toString(), inputRow.get("MESSAGE_DATA"));
			}
		}

		for (Map<String, Object> row : feedbackList) {
			Object txId = row.get("TRANSACTION_ID");
			row.put("PROMPT", txId == null ? null : txIdToInputData.get(txId.toString()));
		}
	}

	/**
	 * Returns aggregate feedback counts for the admin dashboard: total, positive,
	 * and negative counts, optionally grouped/filtered by project, user, engine, or
	 * date range.
	 *
	 * @param startDate inclusive lower bound on FEEDBACK_DATE (nullable)
	 * @param endDate   inclusive upper bound on FEEDBACK_DATE (nullable)
	 * @param projectId filter by project (nullable)
	 * @param userId    filter by user (nullable)
	 * @param engineId  filter by agent/engine (nullable)
	 * @return list of count maps with TOTAL_FEEDBACK, POSITIVE_FEEDBACK,
	 *         NEGATIVE_FEEDBACK
	 */
	public static List<Map<String, Object>> getFeedbackCountForAdmin(String startDate, String endDate, String projectId,
			String userId, String engineId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		SelectQueryStruct qs = new SelectQueryStruct();

		// Total feedback count
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT,
				FEEDBACK_TABLE_NAME + "MESSAGE_ID", "TOTAL_FEEDBACK"));

		// Positive feedback count: SUM(CASE WHEN RATING = true THEN 1 ELSE 0 END)
		QueryIfSelector positiveIf = new QueryIfSelector();
		positiveIf.setCondition(SimpleQueryFilter.makeColToValFilter(FEEDBACK_TABLE_NAME + "RATING", "==", true));
		positiveIf.setPrecedent(new QueryConstantSelector(1));
		positiveIf.setAntecedent(new QueryConstantSelector(0));

		qs.addSelector(
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM, positiveIf, "POSITIVE_FEEDBACK"));

		// Negative feedback count: SUM(CASE WHEN RATING = false THEN 1 ELSE 0 END)
		QueryIfSelector negativeIf = new QueryIfSelector();
		negativeIf.setCondition(SimpleQueryFilter.makeColToValFilter(FEEDBACK_TABLE_NAME + "RATING", "==", false));
		negativeIf.setPrecedent(new QueryConstantSelector(1));
		negativeIf.setAntecedent(new QueryConstantSelector(0));

		qs.addSelector(
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.SUM, negativeIf, "NEGATIVE_FEEDBACK"));

		// Join FEEDBACK -> MESSAGE on MESSAGE_ID
		qs.addRelation(FEEDBACK_TABLE_NAME + "MESSAGE_ID", MESSAGE_TABLE_NAME + "MESSAGE_ID", "inner.join");
		// Join MESSAGE -> ROOM on ROOM_ID
		qs.addRelation(MESSAGE_TABLE_NAME + "ROOM_ID", ROOM_TABLE_NAME + "ROOM_ID", "left.join");

		// Optional filters
		if (projectId != null && !projectId.trim().isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ROOM_TABLE_NAME + "PROJECT_ID", "==", projectId));
		}
		if (userId != null && !userId.trim().isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "USER_ID", "==", userId));
		}
		if (engineId != null && !engineId.trim().isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "AGENT_ID", "==", engineId));
		}

		// Date range filter on FEEDBACK_DATE
		addFeedbackDateFilter(qs, startDate, endDate);

		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * Adds a date range filter on FEEDBACK__FEEDBACK_DATE, comparing only the date
	 * portion (time is stripped via CAST). Both bounds are inclusive.
	 *
	 * @param qs        the query struct to modify
	 * @param startDate inclusive lower bound (nullable)
	 * @param endDate   inclusive upper bound (nullable)
	 */
	private static void addFeedbackDateFilter(SelectQueryStruct qs, String startDate, String endDate) {
		boolean hasStart = startDate != null && !startDate.trim().isEmpty();
		boolean hasEnd = endDate != null && !endDate.trim().isEmpty();
		if (!hasStart && !hasEnd) {
			return;
		}
		if (hasStart ^ hasEnd) {
			throw new IllegalArgumentException(
					"Both startDate and endDate must be provided for the feedback date filter");
		}

		// Build a CAST(FEEDBACK.FEEDBACK_DATE AS DATE) selector so we compare
		// only the date portion, making both start and end fully inclusive.
		QueryFunctionSelector castSelector = new QueryFunctionSelector();
		castSelector.setFunction(QueryFunctionHelper.CAST);
		castSelector.addInnerSelector(new QueryColumnSelector(FEEDBACK_TABLE_NAME + "FEEDBACK_DATE"));
		castSelector.setDataType("DATE");

		AndQueryFilter andFilters = new AndQueryFilter();
		andFilters.addFilter(
				SimpleQueryFilter.makeColToValFilter(castSelector, ">=", startDate, PixelDataType.CONST_STRING));
		andFilters.addFilter(
				SimpleQueryFilter.makeColToValFilter(castSelector, "<=", endDate, PixelDataType.CONST_STRING));
		qs.addExplicitFilter(andFilters);
	}

	// ============================================================
	// Skills
	//
	// A skill is a Project of type SKILL; the Project owns content (SKILL.md
	// under version/assets/skill/), versioning (git in version/), and
	// permissions. WORKSPACE_RESOURCE__ rows with RESOURCE_TYPE='SKILL' attach
	// skills to workspaces, mirrored into CONFIG_JSON.skills[].
	// ============================================================

	/**
	 * Detaches a skill project from every workspace that references it: deletes the
	 * WORKSPACE_RESOURCE__ rows (RESOURCE_TYPE='SKILL') and scrubs the matching
	 * {@code CONFIG_JSON.skills[]} mirror entry in each affected workspace. Does
	 * NOT delete the underlying Project - callers (typically the project-delete
	 * path) own that.
	 *
	 * <p>
	 * Attach writes the WORKSPACE_RESOURCE row AND the CONFIG_JSON.skills[] mirror
	 * (see {@code AttachSkillToWorkspaceReactor}); delete must clear BOTH. Skipping
	 * the mirror leaves a dangling skill id that
	 * {@code AgentConfigLoader.resolveSkills} still returns, so the run-time
	 * {@code SkillStager} fails it every run. The referencing workspaces are
	 * captured before the row deletes so the mirror can be scrubbed afterward.
	 *
	 * @param projectId skill project identifier
	 */
	public static void detachSkillFromAllWorkspaces(String projectId) {
		if (!SystemEngineRegistry.isModelInferenceLogsDbLoaded()) {
			classLogger.warn("Model inference logs db is not loaded; skipping workspace scrub for skill '{}'",
					projectId);
			return;
		}
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		Connection con = null;
		List<String> affectedWorkspaceIds = new ArrayList<>();
		try {
			con = modelInferenceLogsDb.getConnection();
			try (PreparedStatement sel = con.prepareStatement(
					"SELECT DISTINCT WORKSPACE_ID FROM WORKSPACE_RESOURCE WHERE RESOURCE_TYPE = ? AND RESOURCE_ID = ?")) {
				sel.setString(1, "SKILL");
				sel.setString(2, projectId);
				try (ResultSet rs = sel.executeQuery()) {
					while (rs.next()) {
						String wsId = rs.getString(1);
						if (wsId != null && !wsId.trim().isEmpty()) {
							affectedWorkspaceIds.add(wsId);
						}
					}
				}
			}
			try (PreparedStatement ps = con
					.prepareStatement("DELETE FROM WORKSPACE_RESOURCE WHERE RESOURCE_TYPE = ? AND RESOURCE_ID = ?")) {
				ps.setString(1, "SKILL");
				ps.setString(2, projectId);
				ps.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to detach skill '{}' from workspaces.", projectId, e);
			throw new IllegalArgumentException("Error detaching skill from workspaces: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}

		for (String workspaceId : affectedWorkspaceIds) {
			try {
				removeSkillFromWorkspaceConfigJson(workspaceId, projectId);
			} catch (Exception e) {
				classLogger.warn(
						"Detached skill '{}' but failed to scrub it from CONFIG_JSON.skills[] for workspace '{}': {}",
						projectId, workspaceId, e.getMessage());
			}
		}
	}

}
