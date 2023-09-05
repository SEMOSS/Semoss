package prerna.engine.impl.model.inferencetracking;

import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class ModelInferenceLogsUtils {
	
	private static Logger classLogger = LogManager.getLogger(ModelInferenceLogsUtils.class);
	
	static IRDBMSEngine modelInferenceLogsDb;
	static boolean initialized = false;
	
	// this is good for python dictionaries but also for making sure we can easily construct 
	// the logs into model inference python list, since everything is python at this point.
    public static String constructPyDictFromMap(Map<String,Object> theMap) {
    	StringBuilder theDict = new StringBuilder("{");
    	boolean isFirstElement = true;
    	for (Entry<String, Object> entry : theMap.entrySet()) {
    		if (!isFirstElement) {
    			theDict.append(",");
    		} else {
    			isFirstElement = false;
    		}
    		theDict.append(determineStringType(entry.getKey())).append(":").append(determineStringType(entry.getValue()));
    		//theDict.append(determineStringType(entry.getKey())).append(":").append(determineStringType(entry.getValue())).append(",");
    	}
    	theDict.append("}");
    	return theDict.toString();
    }
    
    /* This is basically a utility method that attemps to generate the python code (string) for a java object.
	 * It currently only does base types.
	 * Potentially move it in the future but just keeping it here for now
	*/
    @SuppressWarnings("unchecked")
    public static String determineStringType(Object obj) {
    	if (obj instanceof Integer || obj instanceof Double || obj instanceof Long) {
    		return String.valueOf(obj);
    	} else if (obj instanceof Map) {
    		return constructPyDictFromMap((Map<String, Object>) obj);
    	} else if (obj instanceof ArrayList || obj instanceof Object[] || obj instanceof List) {
    		StringBuilder theList = new StringBuilder("[");
    		List<Object> list;
    		if (obj instanceof ArrayList<?>) {
    			list = (ArrayList<Object>) obj;
    		} else if ((obj instanceof Object[])) {
    			list = Arrays.asList((Object[]) obj);
    		} else {
    			list = (List<Object>) obj;
    		}
    		
    		boolean isFirstElement = true;
			for (Object subObj : list) {
				if (!isFirstElement) {
					theList.append(",");
	    		} else {
	    			isFirstElement = false;
	    		}
				theList.append(determineStringType(subObj));
        	}
			theList.append("]");
			return theList.toString();
    	} else if (obj instanceof Boolean) {
    		String boolString = String.valueOf(obj);
    		// convert to py version
    		String cap = boolString.substring(0, 1).toUpperCase() + boolString.substring(1);
    		return cap;
    	} else if (obj instanceof Set<?>) {
    		StringBuilder theSet = new StringBuilder("{");
    		Set<?> set = (Set<?>) obj;
    		boolean isFirstElement = true;
			for (Object subObj : set) {
				if (!isFirstElement) {
					theSet.append(",");
				} else {
					isFirstElement = false;
				}
				theSet.append(determineStringType(subObj));
        	}
			theSet.append("}");
			return theSet.toString();
    	} else {
    		return "\'"+String.valueOf(obj).replace("'", "\\'").replace("\n", "\\n") + "\'";
    	}
    }
    
    public static Integer getTokenSizeString(String input) {
        if (input == null || input.trim().isEmpty()) {
            return 0;
        }
        //TODO should we be using the model tokenizer?
        StringTokenizer str_arr = new StringTokenizer(input);
        return str_arr.countTokens();
    }
	
	public static String generateRoomTitle(AbstractModelEngine engine, String originalQuestion) {
		StringBuilder summarizeStatement = new StringBuilder("summarize \\\"");
		summarizeStatement.append(originalQuestion);
		summarizeStatement.append("\\\" in less than 8 words. Please exclude all punctuation from the response.");
		String roomTitle = engine.askQuestion(summarizeStatement.toString(), null, null, null);
		return roomTitle;
	}
	
	public static boolean userIsMessageAuthor(String userId, String messageId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector newSelector = new QueryFunctionSelector();
		newSelector.setAlias("Counts");
		newSelector.setFunction(QueryFunctionHelper.COUNT);
		newSelector.addInnerSelector(new QueryColumnSelector("MESSAGE__MESSAGE_ID"));

		qs.addSelector(newSelector);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_ID", "==", messageId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__USER_ID", "==", userId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs);
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return false;
				}
				int intVal = ((Number) val).intValue();
				if(intVal > 0) {
					return true;
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return false;
	}
	
	public static void recordFeedback(String messageId, String feedbackText, boolean rating) {
		if (feedbackExists(messageId)) {
			updateFeedback(messageId, feedbackText, rating);
		} else {
			insertFeedback(messageId, feedbackText, rating);
		}
	}
	
	public static boolean feedbackExists(String messageId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector newSelector = new QueryFunctionSelector();
		newSelector.setAlias("Counts");
		newSelector.setFunction(QueryFunctionHelper.COUNT);
		newSelector.addInnerSelector(new QueryColumnSelector("FEEDBACK__MESSAGE_ID"));

		qs.addSelector(newSelector);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("FEEDBACK__MESSAGE_ID", "==", messageId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("FEEDBACK__MESSAGE_TYPE", "==", "RESPONSE"));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs);
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return false;
				}
				int intVal = ((Number) val).intValue();
				if(intVal > 0) {
					return true;
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return false;
	}
	
	public static void insertFeedback(String messageId, String feedbackText, boolean rating) {
		String query = "INSERT INTO FEEDBACK (MESSAGE_ID, MESSAGE_TYPE, FEEDBACK_TEXT, FEEDBACK_DATE, RATING) "
				+ "VALUES (?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, messageId);
			ps.setString(index++, "RESPONSE");
			ps.setString(index++, feedbackText);
			ps.setObject(index++, LocalDateTime.now());
			ps.setBoolean(index++, rating);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}	
	}
	
	public static void updateFeedback(String messageId, String feedbackText, boolean rating) {
		String query = "UPDATE FEEDBACK SET FEEDBACK_TEXT =?, "
				+ "FEEDBACK_DATE = ?, RATING = ? WHERE MESSAGE_ID = ? AND MESSAGE_TYPE = 'RESPONSE'";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, feedbackText);
			ps.setObject(index++, LocalDateTime.now());
			ps.setBoolean(index++, rating);
			ps.setString(index++, messageId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}	
	}
	
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}	
	}
	
	public static String doCreateNewConversation(String roomName, String roomContext,
			String userId, String agentType, Boolean isActive, String projectId, String projectName, String agentId) {
		String convoId = UUID.randomUUID().toString();
		doCreateNewConversation(convoId, roomName, roomContext, userId, agentType, isActive, projectId, projectName, agentId);
		return convoId;
	}
	
	public static void doCreateNewConversation(String insightId, String roomName, String roomContext, 
											   String userId, String agentType, Boolean isActive, String projectId, String projectName, String agentId) {
		String query = "INSERT INTO ROOM (INSIGHT_ID, ROOM_NAME, "
				+ "ROOM_CONTEXT, USER_ID, AGENT_TYPE, IS_ACTIVE, "
				+ "DATE_CREATED, PROJECT_ID, PROJECT_NAME, AGENT_ID) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		// boolean allowClob = modelInferenceLogsDb.getQueryUtil().allowClobJavaObject();
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, insightId);
			if (roomName != null) {
				ps.setString(index++, roomName);
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			if (roomContext != null) {
				ps.setString(index++, roomContext);
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
//			if (roomConfigData != null) {
//				if(allowClob) {
//					Clob toclob = modelInferenceLogsDb.getConnection().createClob();
//					toclob.setString(1, roomConfigData);
//					ps.setClob(index++, toclob);
//				} else {
//					ps.setString(index++, roomConfigData);
//				}
//			} else {
//				ps.setNull(index++, java.sql.Types.NULL);
//			}
			ps.setString(index++, userId);
			ps.setString(index++, agentType);
			ps.setBoolean(index++, isActive);
			ps.setObject(index++, LocalDateTime.now());
			ps.setString(index++, projectId);
			ps.setString(index++, projectName);
			ps.setString(index++, agentId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}
	
	public static boolean doCheckConversationExists (String roomId) {
		String query = "SELECT COUNT(*) FROM ROOM WHERE INSIGHT_ID = ?";
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
		return false;
	}
	
	public static String doCreateNewAgent(String agentName, String agentDescription, String agentType,
			String author) {
		String agentId = UUID.randomUUID().toString();
		doCreateNewAgent(agentId, agentName, agentDescription, agentType, author);
		return agentId;
	}
	
	public static void doCreateNewAgent(String agentId, String agentName, String agentDescription, String agentType,
			 String author) {
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
			ps.setObject(index++, LocalDateTime.now());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}
	
	public static void doRecordMessage(String messageId,
									   String messageType,
									   String messageData,
									   String messageMethod,
									   Integer tokenSize,
									   Double reponseTime,
									   String agentId,
									   String insightId,
									   String sessionId,
									   String userId) {
		LocalDateTime dateCreated = LocalDateTime.now();
		doRecordMessage(messageId, messageType, messageData, messageMethod, tokenSize, reponseTime, dateCreated, agentId, insightId, sessionId, userId);
	}
	
	public static void doRecordMessage(String messageId,
									   String messageType,
									   String messageData,
									   String messageMethod,
									   Integer tokenSize,
									   Double reponseTime,
									   LocalDateTime dateCreated,
									   String agentId,
									   String insightId,
									   String sessionId,
									   String userId) {
		boolean allowClob = modelInferenceLogsDb.getQueryUtil().allowClobJavaObject();
		String query = "INSERT INTO MESSAGE (MESSAGE_ID, MESSAGE_TYPE, MESSAGE_DATA, MESSAGE_METHOD, MESSAGE_TOKENS, RESPONSE_TIME,"
			+ " DATE_CREATED, AGENT_ID, INSIGHT_ID, SESSIONID, USER_ID) " + 
			"	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, messageId);
			ps.setString(index++, messageType);
			if (messageData != null) {
				if(allowClob) {
					Clob toclob = modelInferenceLogsDb.getConnection().createClob();
					toclob.setString(1, messageData);
					ps.setClob(index++, toclob);
				} else {
					ps.setString(index++, messageData);
				}
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			ps.setString(index++, messageMethod);
			ps.setInt(index++, tokenSize);
			ps.setDouble(index++, reponseTime);
			ps.setObject(index++, dateCreated);
			ps.setString(index++, agentId);
			ps.setString(index++, insightId);
			ps.setString(index++, sessionId);
			ps.setString(index++, userId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}
	
	public static boolean doSetRoomToInactive(String userId, String roomId) {
		Connection conn = connectToInferenceLogs();
		String query = "UPDATE ROOM SET IS_ACTIVE = false WHERE USER_ID = ? AND INSIGHT_ID = ?";
		try (PreparedStatement ps = conn.prepareStatement(query)) {
			//ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, userId);
			ps.setString(index++, roomId);
			ps.executeUpdate();
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		} finally {
			if(modelInferenceLogsDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return true;
	}
	
	
	public static List<Map<String, Object>> doRetrieveConversation(String userId, String insightId, String dateSort) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("MESSAGE__DATE_CREATED"));
		qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_TYPE"));
		qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_DATA"));
		qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__INSIGHT_ID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__USER_ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_METHOD", "==", "ask"));
		qs.addOrderBy(new QueryColumnOrderBySelector("MESSAGE__DATE_CREATED", dateSort));
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}
	
	public static List<Map<String, Object>> doVerifyConversation(String userId, String insightId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ROOM__INSIGHT_ID"));
		qs.addSelector(new QueryColumnSelector("ROOM__PROJECT_ID"));
		
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__INSIGHT_ID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));
		qs.setDistinct(true);
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}
	
	public static List<Map<String, Object>> getUserConversations(String userId, String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ROOM__INSIGHT_ID","ROOM_ID"));
		qs.addSelector(new QueryColumnSelector("ROOM__ROOM_NAME"));
		qs.addSelector(new QueryColumnSelector("ROOM__ROOM_CONTEXT"));
		qs.addSelector(new QueryColumnSelector("ROOM__AGENT_ID","MODEL_ID"));
		qs.addSelector(new QueryColumnSelector("ROOM__DATE_CREATED"));
		qs.addRelation("ROOM__INSIGHT_ID", "MESSAGE__INSIGHT_ID", "inner.join");
		
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__IS_ACTIVE", "==", true));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_DATA", "!=", true));
		if (projectId != null) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__PROJECT_ID", "==", projectId));
		}
		qs.addOrderBy(new QueryColumnOrderBySelector("ROOM__DATE_CREATED", "DESC"));
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}
	
	// TODO add ability to swap out database
	public static Connection connectToInferenceLogs() {
		Connection connection = null;

		try {
			modelInferenceLogsDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.MODEL_INFERENCE_LOGS_DB);
			connection = modelInferenceLogsDb.getConnection();
		} catch (SQLException se) {
			classLogger.error(Constants.STACKTRACE, se);
		} catch (Exception ex) {
			classLogger.error(Constants.STACKTRACE, ex);
		}

		if (connection == null) {
			throw new NullPointerException("Connection wasn't able to be created.");
		}

		return connection;
	}
	
	public static void initModelInferenceLogsDatabase() throws Exception {
		modelInferenceLogsDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.MODEL_INFERENCE_LOGS_DB);
		ModelInferenceLogsOwlCreation modelInfCreator = new ModelInferenceLogsOwlCreation(modelInferenceLogsDb);
		if(modelInfCreator.needsRemake()) {
			modelInfCreator.remakeOwl();
			// reset the local master metadata for model engine if we remade the OWL
			Utility.synchronizeEngineMetadata(Constants.MODEL_INFERENCE_LOGS_DB);
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
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, conn, null, null);
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
					classLogger.error(Constants.STACKTRACE, se);
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
					classLogger.error(Constants.STACKTRACE, se);
				}
			} else {
				String primaryKeyQuery = "ALTER TABLE " + tableName + " ADD CONSTRAINT " + primaryKeyConstraintName + " PRIMARY KEY ( " + String.join(",", primaryKeyNames) +  " );";
				try {
					if(!queryUtil.tableConstraintExists(conn, primaryKeyConstraintName, tableName, engine.getDatabase(), engine.getSchema())) {
						executeSql(conn, primaryKeyQuery);
					}
				} catch (SQLException se) {
					classLogger.error(Constants.STACKTRACE, se);
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
				constraintName = constraintName.replace(",", "");
				if(engine.getQueryUtil().allowIfExistsAddConstraint()) {
					String sqlStatement = String.format(
			                "ALTER TABLE %s ADD CONSTRAINT IF NOT EXISTS %s FOREIGN KEY (%s) REFERENCES %s (%s);",
			                tableName, constraintName, tableColumn, refTable, refColumn);
					try {
						executeSql(conn, sqlStatement);
					} catch (SQLException se) {
						classLogger.error(Constants.STACKTRACE, se);
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
						classLogger.error(Constants.STACKTRACE, se);
						break ATTEMPT_TO__ADD_FOREIGN_KEY; // most likely incorrect syntax
					}
				}
			}
		}
	}
	
	private static void executeSql(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			classLogger.info("Running sql " + sql);
			stmt.execute(sql);
		}
	}
}
