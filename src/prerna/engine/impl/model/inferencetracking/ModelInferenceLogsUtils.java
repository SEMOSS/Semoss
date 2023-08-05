package prerna.engine.impl.model.inferencetracking;

import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;



public class ModelInferenceLogsUtils {
	
	private static Logger logger = LogManager.getLogger(ModelInferenceLogsUtils.class);
	static IRDBMSEngine modelInferenceLogsDb;
	static boolean initialized = false;
	
	public static void doCreateNewUser(User user) {
		String query = "INSERT INTO USERS (USER_ID, USERNAME, EMAIL) VALUES (?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, user.getPrimaryLoginToken().getId());
			ps.setString(index++, user.getPrimaryLoginToken().getUsername());
			ps.setString(index++, user.getPrimaryLoginToken().getEmail());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeResources(modelInferenceLogsDb, null, ps, null);
		}	
	}
	
	public static String doCreateNewConversation(String roomName, String roomDescription, 
			   String roomConfigData, String userId, String agentType, Boolean isActive, String projectId, String agentId) {
		String convoId = UUID.randomUUID().toString();
		doCreateNewConversation(convoId, roomName, roomDescription, roomConfigData, userId, agentType, isActive, projectId, agentId);
		return convoId;
	}
	
	public static void doCreateNewConversation(String roomId, String roomName, String roomDescription, 
											   String roomConfigData, String userId, String agentType, Boolean isActive, String projectId, String agentId) {
		String query = "INSERT INTO ROOM (ROOM_ID, ROOM_NAME, "
				+ "ROOM_DESCRIPTION, ROOM_CONFIG_DATA, USER_ID, AGENT_TYPE, IS_ACTIVE, "
				+ "DATE_CREATED, PROJECT_ID, AGENT_ID) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		boolean allowClob = modelInferenceLogsDb.getQueryUtil().allowClobJavaObject();
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, roomId);
			ps.setString(index++, roomName);
			ps.setString(index++, roomDescription);
			if(allowClob) {
				Clob toclob = modelInferenceLogsDb.getConnection().createClob();
				toclob.setString(1, roomConfigData);
				ps.setClob(index++, toclob);
			} else {
				ps.setString(index++, roomConfigData);
			}
			ps.setString(index++, userId);
			ps.setString(index++, userId);
			ps.setBoolean(index++, isActive);
			ps.setObject(index++, LocalDateTime.now());
			ps.setString(index++, projectId);
			ps.setString(index++, agentId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeResources(modelInferenceLogsDb, null, ps, null);
		}
	}
	
	public static boolean doCheckConversationExists (String roomId) {
		String query = "SELECT COUNT(*) FROM ROOM WHERE ROOM_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, roomId);
			ps.execute();
			if(ps.execute()) {
				ResultSet rs = ps.getResultSet();
				if (rs.next()) {
					int count = rs.getInt(1);
					return count >= 1;
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeResources(modelInferenceLogsDb, null, ps, null);
		}
		return false;
	}
	
	public static boolean doModelIsRegistered (String agentId) {
		String query = "SELECT COUNT(*) FROM AGENT WHERE AGENT_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, agentId);
			ps.execute();
			if(ps.execute()) {
				ResultSet rs = ps.getResultSet();
				if (rs.next()) {
					int count = rs.getInt(1);
					return count >= 1;
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeResources(modelInferenceLogsDb, null, ps, null);
		}
		return false;
	}
	
	public static String doCreateNewAgent(String agentName, String agentDescription, String agentType,
			Boolean isMarketplace, String author) {
		String agentId = "EMB_" + UUID.randomUUID().toString();
		doCreateNewAgent(agentId, agentName, agentDescription, agentType, isMarketplace, author);
		return agentId;
	}
	
	public static void doCreateNewAgent(String agentId, String agentName, String agentDescription, String agentType,
			Boolean isMarketplace, String author) {
		String query = "INSERT INTO AGENT (AGENT_ID, AGENT_NAME, DESCRIPTION, AGENT_TYPE, "
				+ "MARKETPLACE, AUTHOR, DATE_CREATED) VALUES (?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, agentId);
			ps.setString(index++, agentName);
			ps.setString(index++, agentDescription);
			ps.setString(index++, agentType);
			ps.setBoolean(index++, isMarketplace);
			ps.setString(index++, author);
			ps.setObject(index++, LocalDateTime.now());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeResources(modelInferenceLogsDb, null, ps, null);
		}
	}
	
	public static void doRecordMessage(String messageId,
									   String messageType,
									   String messageData,
									   String roomId,
									   String agentId,
									   String feedbackText,
									   LocalDateTime feedbackDate,
									   Boolean rating,
									   String sessionId,
									   String userId) {
		LocalDateTime dateCreated = LocalDateTime.now();
		doRecordMessage(messageId, messageType, messageData, dateCreated, roomId, agentId, feedbackText, feedbackDate, rating, sessionId, userId);
	}
	
	public static void doRecordMessage(String messageId,
									   String messageType,
									   String messageData,
									   LocalDateTime dateCreated,
									   String roomId,
									   String agentId,
									   String feedbackText,
									   LocalDateTime feedbackDate,
									   Boolean rating,
									   String sessionId,
									   String userId) {
		boolean allowClob = modelInferenceLogsDb.getQueryUtil().allowClobJavaObject();
		String query = "INSERT INTO MESSAGE (MESSAGE_ID, MESSAGE_TYPE, MESSAGE_DATA,"
			+ " DATE_CREATED, ROOM_ID, AGENT_ID, FEEDBACK_TEXT, FEEDBACK_DATE,"
			+ " RATING, SESSIONID, USER_ID) " + 
			"	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, messageId);
			ps.setString(index++, messageType);
			if(allowClob) {
				Clob toclob = modelInferenceLogsDb.getConnection().createClob();
				toclob.setString(1, messageData);
				ps.setClob(index++, toclob);
			} else {
				ps.setString(index++, messageData);
			}
			ps.setObject(index++, dateCreated);
			ps.setString(index++, roomId);
			ps.setString(index++, agentId);
			ps.setString(index++, feedbackText);
			ps.setObject(index++, feedbackDate);
			ps.setBoolean(index++, rating);
			ps.setString(index++, sessionId);
			ps.setString(index++, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeResources(modelInferenceLogsDb, null, ps, null);
		}
	}
	
	public static List<Map<String, Object>> doRetrieveConversation(String roomId, String userId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("MESSAGE__DATE_CREATED"));
		qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_TYPE"));
		qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_DATA"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__ROOM_ID", "==", roomId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__USER_ID", "==", userId));
		qs.addOrderBy(new QueryColumnOrderBySelector("MESSAGE__DATE_CREATED", "ASC"));
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}
	
	public static void initModelInferenceLogsDatabase() throws SQLException, IOException {
		modelInferenceLogsDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.MODEL_INFERENCE_LOGS_DB);
		ModelInferenceLogsOwlCreation modelInfCreator = new ModelInferenceLogsOwlCreation(modelInferenceLogsDb);
		if(modelInfCreator.needsRemake()) {
			modelInfCreator.remakeOwl();
		}
		
		Connection conn = null;
		try {
			conn = modelInferenceLogsDb.makeConnection();
			executeInitModelInferenceDatabase(modelInferenceLogsDb, conn, modelInfCreator.getDBSchema());
			boolean primaryKeysAdded = addAllPrimaryKeys(modelInferenceLogsDb, conn, modelInfCreator.getDBPrimaryKeys());
			if (primaryKeysAdded) {
				addAllForeignKeys(modelInferenceLogsDb, conn, modelInfCreator.getDBForeignKeys());
			}
		} finally {
			closeResources(modelInferenceLogsDb, conn, null, null);
		}
		initialized = true;
	}
	
	/**
	 * 
	 * @param engine
	 * @param conn
	 * @param columnNamesAndTypes
	 * @throws SQLException
	 */
	private static void executeInitModelInferenceDatabase(
			IRDBMSEngine engine, 
			Connection conn,
			List<Pair<String, List<Pair<String, String>>>> dbSchema) throws SQLException {

		String database = engine.getDatabase();
		String schema = engine.getSchema();

		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();

		for (Pair<String, List<Pair<String, String>>> tableSchema : dbSchema) {
			String tableName = tableSchema.getLeft();
			String[] colNames = tableSchema.getRight().stream().map(Pair::getLeft).toArray(String[]::new);
			String[] types = tableSchema.getRight().stream().map(Pair::getRight).toArray(String[]::new);
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
				if(!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
					String addColumnSql = queryUtil.alterTableAddColumn(tableName, col, types[i]);
					executeSql(conn, addColumnSql);
				}
			}
		}
	}
	
	private static boolean addAllPrimaryKeys(IRDBMSEngine engine, Connection conn, List<Pair<String, Pair<List<String>, List<String>>>> primaryKeys) {
		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		for (Pair<String, Pair<List<String>, List<String>>> tablePrimaryKeys : primaryKeys) {
			String tableName = tablePrimaryKeys.getLeft();
			Pair<List<String>, List<String>> primaryKeyInfo = tablePrimaryKeys.getRight();
			List<String> primaryKeyNames = primaryKeyInfo.getLeft();
			List<String> primaryKeyTypes = primaryKeyInfo.getRight();
			
			// first try make sure its not null
			for (int i = 0; i < primaryKeyNames.size(); i++) {
				String name = primaryKeyNames.get(i);
				String type = primaryKeyTypes.get(i);
				String notNullQuery = "ALTER TABLE " + tableName + " ALTER COLUMN " + name + " " + type +  " NOT NULL;";
				try {
					executeSql(conn, notNullQuery);
				} catch (SQLException se) {
					logger.error(Constants.STACKTRACE, se);
					// We can't change it to NOT NULL so probably can't create the PRIMARY KEY
					return true;
				}
			}
			String primaryKeyConstraintName = tableName + "_KEY";
			if(queryUtil.allowIfExistsAddConstraint()) {
				String primaryKeyQuery = "ALTER TABLE " + tableName + " ADD CONSTRAINT IF NOT EXISTS " + primaryKeyConstraintName + " PRIMARY KEY ( " + String.join(",", primaryKeyNames) +  " );";
				try {
					executeSql(conn, primaryKeyQuery);
				} catch (SQLException se) {
					logger.error(Constants.STACKTRACE, se);
				}
			} else {
				String primaryKeyQuery = "ALTER TABLE " + tableName + " ADD CONSTRAINT " + primaryKeyConstraintName + " PRIMARY KEY ( " + String.join(",", primaryKeyNames) +  " );";
				try {
					if(!queryUtil.tableConstraintExists(conn, primaryKeyConstraintName, tableName, engine.getDatabase(), engine.getSchema())) {
						executeSql(conn, primaryKeyQuery);
					}
				} catch (SQLException se) {
					logger.error(Constants.STACKTRACE, se);
				}
			}
		}
		return true;
	}
	
	private static void addAllForeignKeys(IRDBMSEngine engine, Connection conn, 
			List<Pair<String, Pair<List<String>, Pair<List<String>, List<String>>>>> foreignKeys) {
		ATTEMPT_TO__ADD_FOREIGN_KEY : for (Pair<String, Pair<List<String>, Pair<List<String>, List<String>>>> tableForeignKeys : foreignKeys) {
			String tableName = tableForeignKeys.getLeft();
			Pair<List<String>, Pair<List<String>, List<String>>> foreignKeyInfo = tableForeignKeys.getRight();
			List<String> tableColumns = foreignKeyInfo.getLeft();
			Pair<List<String>, List<String>> referenceDetails = foreignKeyInfo.getRight();
			List<String> referenceTables = referenceDetails.getLeft();
			List<String> referenceColumns = referenceDetails.getRight();
			
			for (int i = 0; i < tableColumns.size(); i++) {
				String tableColumn = tableColumns.get(i);
				String refTable = referenceTables.get(i);
				String refColumn = referenceColumns.get(i);
				
				String constraintName = tableName + "_" + tableColumn + "_" + refTable + "_" + refColumn + "_KEY";
				if(engine.getQueryUtil().allowIfExistsAddConstraint()) {
					String sqlStatement = String.format(
			                "ALTER TABLE %s ADD CONSTRAINT IF NOT EXISTS %s FOREIGN KEY (%s) REFERENCES %s (%s);",
			                tableName, constraintName, tableColumn, refTable, refColumn);
					try {
						executeSql(conn, sqlStatement);
					} catch (SQLException se) {
						logger.error(Constants.STACKTRACE, se);
						break ATTEMPT_TO__ADD_FOREIGN_KEY; // most likely incorrect syntax
					}
				} else {
					String sqlStatement = String.format(
			                "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s);",
			                tableName, constraintName, tableColumn, refTable, refColumn);
					try {
						if(!engine.getQueryUtil().tableConstraintExists(conn, constraintName, tableName, engine.getDatabase(), engine.getSchema())) {
							executeSql(conn, sqlStatement);
						}
					} catch (SQLException se) {
						logger.error(Constants.STACKTRACE, se);
						break ATTEMPT_TO__ADD_FOREIGN_KEY; // most likely incorrect syntax
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param engine
	 * @param conn
	 * @param stmt
	 * @param rs
	 */
	private static void closeResources(IRDBMSEngine engine, Connection conn, Statement stmt, ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		try {
			if (stmt != null) {
				stmt.close();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		try {
			if (engine != null && engine.isConnectionPooling()) {
				if(conn != null) {
					conn.close();
				} else if(stmt != null) {
					stmt.getConnection().close();
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

	private static void executeSql(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			logger.info("Running sql " + sql);
			stmt.execute(sql);
		}
	}
}
