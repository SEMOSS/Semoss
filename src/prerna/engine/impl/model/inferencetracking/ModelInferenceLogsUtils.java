package prerna.engine.impl.model.inferencetracking;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class ModelInferenceLogsUtils {
	
	private static Logger classLogger = LogManager.getLogger(ModelInferenceLogsUtils.class);
	
	// Constants for Table 
	private static final String MESSAGE_TABLE_NAME = "MESSAGE__";
	private static final String AGENT_TABLE_NAME = "AGENT__";
	private static final String ROOM_TABLE_NAME = "ROOM__";
	
	static IRDBMSEngine modelInferenceLogsDb;
	static boolean initialized = false;
	
    public static Integer getTokenSizeString(String input) {
        if (input == null || input.trim().isEmpty()) {
            return 0;
        }
        //TODO should we be using the model tokenizer?
        StringTokenizer str_arr = new StringTokenizer(input);
        return str_arr.countTokens();
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
			ps.setTimestamp(index++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
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
		Connection conn = connectToInferenceLogs();
		try {
			UpdateQueryStruct qs = new UpdateQueryStruct();
			qs.setEngine(modelInferenceLogsDb);
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("FEEDBACK__MESSAGE_ID", "==", messageId));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("FEEDBACK__MESSAGE_TYPE", "==", "RESPONSE"));
			List<IQuerySelector> selectors = new ArrayList<>(
					Arrays.asList(
							new QueryColumnSelector("FEEDBACK__FEEDBACK_TEXT"), 
							new QueryColumnSelector("FEEDBACK__FEEDBACK_DATE"), 
							new QueryColumnSelector("FEEDBACK__RATING")
							)
					);

			List<Object> values = new ArrayList<>(Arrays.asList(feedbackText, new SemossDate(Utility.getCurrentZonedDateTimeUTC()), rating));

			qs.setSelectors(selectors);
			qs.setValues(values);
			qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
			UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(qs);
			String updateQ = updateInterp.composeQuery();

			modelInferenceLogsDb.insertData(updateQ);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(modelInferenceLogsDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	
	
	/**
	 * USAGE HELPER FUNCTIONS  
	 * 
	 */
	
	/**
	 * Function returns the number of unique calls (Inputs) per a model 
	 * 
	 * @param engineId
	 * @param offset 
	 * @param limit 
	 * @param dateFilter 
	 * @return
	 */
	public static List<Map<String, Object>> getOverAllEngineUsageFromModelInferenceLogs(String engineId, String limit, String offset, String startDate, String endDate) {
	
		SelectQueryStruct qs = new SelectQueryStruct();
		
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_ID"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_TYPE"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_TOKENS"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_METHOD"));
		qs.addSelector(new QueryColumnSelector(AGENT_TABLE_NAME + "AGENT_NAME"));
		qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_NAME"));
		
		qs.addRelation(MESSAGE_TABLE_NAME + "AGENT_ID", AGENT_TABLE_NAME + "AGENT_ID", "left.join");
		qs.addRelation(MESSAGE_TABLE_NAME + "AGENT_ID", ROOM_TABLE_NAME + "AGENT_ID", "left.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__AGENT_ID", "==", engineId));
		addStartDateEndDateFitler(qs, startDate, endDate);
//		if(dateFilter != null && !dateFilter.trim().isEmpty()) {
//			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "DATE_CREATED", ">=", dateFilter));
//		}
		
		addLimitAndOffSet(qs, limit, offset);
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}
	
	/**
	 * Returns a list of total tokens used per project for engineId passed in 
	 * @param engineId
	 * @param dateFilter 
	 * @param offset 
	 * @param limit 
	 * @return
	 */
	public static List<Map<String, Object>> getTokenUsagePerProjectForEngine(String engineId, String limit, String offset, String startDate, String endDate) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_NAME"));
		
		QueryFunctionSelector sumTokenSelector = new QueryFunctionSelector();
		sumTokenSelector.setAlias("TOTAL_NUMBER_OF_TOKENS");
		sumTokenSelector.setFunction(QueryFunctionHelper.SUM);
		sumTokenSelector.addInnerSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_TOKENS"));
		qs.addSelector(sumTokenSelector);
		
		QueryFunctionSelector countNumberRequestSelector = new QueryFunctionSelector();
		countNumberRequestSelector.setAlias("TOTAL_NUMBER_OF_REQUEST");
		countNumberRequestSelector.setFunction(QueryFunctionHelper.COUNT);
		countNumberRequestSelector.addInnerSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_ID"));
		qs.addSelector(countNumberRequestSelector);
		
		qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_ID"));
		qs.addRelation(MESSAGE_TABLE_NAME + "AGENT_ID", ROOM_TABLE_NAME + "AGENT_ID", "left.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "AGENT_ID", "==", engineId));
		addStartDateEndDateFitler(qs, startDate, endDate);
//		if(dateFilter != null && !dateFilter.trim().isEmpty()) {
//			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "DATE_CREATED", ">=", dateFilter));
//		}
		
		addLimitAndOffSet(qs, limit, offset);
		qs.addGroupBy(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_NAME"));
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}

	/**
	 * @param qs
	 * @param limit
	 * @param offset
	 */
	private static void addLimitAndOffSet(SelectQueryStruct qs, String limit, String offset) {
		Long long_limit = -1L;
		Long long_offset = -1L;
		if(limit != null && !limit.trim().isEmpty()) {
			long_limit = ((Number) Double.parseDouble(limit)).longValue();
		}
		if(offset != null && !offset.trim().isEmpty()) {
			long_offset = ((Number) Double.parseDouble(offset)).longValue();
		}
		qs.setLimit(long_limit);
		qs.setOffSet(long_offset);
	}
	
	/**
	 * 
	 * @param engineId
	 * @param limit
	 * @param offset
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public static List<Map<String, Object>> getUserUsagePerEngine(String engineId, String limit, String offset, String startDate, String endDate) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_NAME"));
		qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_ID"));
		
		QueryFunctionSelector sumTokenSelector = new QueryFunctionSelector();
		sumTokenSelector.setAlias("TOTAL_NUMBER_OF_TOKENS");
		sumTokenSelector.setFunction(QueryFunctionHelper.SUM);
		sumTokenSelector.addInnerSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_TOKENS"));
		qs.addSelector(sumTokenSelector);
		
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "AGENT_ID", "==", engineId));
		addStartDateEndDateFitler(qs, startDate, endDate);
//		if(dateFilter != null && !dateFilter.trim().isEmpty()) {
//			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "DATE_CREATED", ">=", dateFilter));
//		}
		
		addLimitAndOffSet(qs, limit, offset);
		qs.addGroupBy(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_NAME"));
		
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
	}
	
	/**
	 * 
	 * @param qs
	 * @param startDate
	 * @param endDate
	 */
	private static void addStartDateEndDateFitler(SelectQueryStruct qs, String startDate, String endDate) {
		if((startDate != null && !startDate.trim().isEmpty()) && (endDate != null && !endDate.trim().isEmpty())) {
			AndQueryFilter andFilters = new AndQueryFilter();	
			andFilters.addFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "DATE_CREATED", ">=", startDate));
			andFilters.addFilter(SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "DATE_CREATED", "<=", endDate));
			qs.addExplicitFilter(andFilters);
		}
		
	}

	public static Map<String, Object> getProjectUsageFromModelInferenceLogs(String projectId) {
		//TODO - Figure out what exactly we mean by usage cause i have no idea 
		// TODO - take in limit and offset 
		// First get a list of insightIDs from Room 
		List<String> insightIdList = getInsightIdListPerProject(projectId);
		// Second query against message to find number of unique calls? Not sure what we are tracking from projects just yet 
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector newSelector = new QueryFunctionSelector();
		newSelector.setAlias("Unique_Calls");
		newSelector.setFunction(QueryFunctionHelper.COUNT);
		newSelector.addInnerSelector(new QueryColumnSelector("MESSAGE__MESSAGE_ID"));

		qs.addSelector(newSelector);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__INSIGHT_ID", "==", insightIdList));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_TYPE", "==", "INPUT"));
		return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs).get(0);
	}
	
	public static List<String> getInsightIdListPerProject(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ROOM__INSIGHT_ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__PROJECT_ID", "==", projectId));
		List<String> insightIdList = QueryExecutionUtility.flushToListString(modelInferenceLogsDb, qs);
		return insightIdList;
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
												 String userId, String userName, String agentType, 
												 Boolean isActive, String projectId, String projectName, String agentId) {
		String convoId = UUID.randomUUID().toString();
		doCreateNewConversation(convoId, roomName, roomContext, userId, userName, agentType, isActive, projectId, projectName, agentId);
		return convoId;
	}
	
	public static void doCreateNewConversation(String insightId, String roomName, String roomContext, 
											   String userId, String userName, String agentType, 
											   Boolean isActive, String projectId, String projectName, String agentId) {
		String query = "INSERT INTO ROOM (INSIGHT_ID, ROOM_NAME, "
				+ "ROOM_CONTEXT, USER_ID, USER_NAME, AGENT_TYPE, IS_ACTIVE, "
				+ "DATE_CREATED, PROJECT_ID, PROJECT_NAME, AGENT_ID) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps.getConnection(), ps, roomContext, index++, new Gson());
			} else {
				ps.setNull(index++, java.sql.Types.NULL);
			}
			ps.setString(index++, userId);
			ps.setString(index++, userName);
			ps.setString(index++, agentType);
			ps.setBoolean(index++, isActive);
			ps.setTimestamp(index++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
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
	
	public static void setRoomContext(String insightId, String userId, String context) {
		Connection conn = connectToInferenceLogs();
        try {
			UpdateQueryStruct qs = new UpdateQueryStruct();
			qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
			qs.setEngine(modelInferenceLogsDb);
			
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__INSIGHT_ID", "==", insightId));
			List<IQuerySelector> selectors = new ArrayList<>();
			List<Object> values = new ArrayList<>();
			selectors.add(new QueryColumnSelector("ROOM__ROOM_CONTEXT"));
			values.add(context);
			qs.setSelectors(selectors);
			qs.setValues(values);
			
			UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(qs);
			String updateQ = updateInterp.composeQuery();

            modelInferenceLogsDb.insertData(updateQ);
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
        } finally {
            if(modelInferenceLogsDb.isConnectionPooling()) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    classLogger.error(Constants.STACKTRACE, e);
                }
            }
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
			ps.setTimestamp(index++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
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
									   String userId,
									   String userName) {
		ZonedDateTime dateCreated = ZonedDateTime.now();
		doRecordMessage(messageId, messageType, messageData, messageMethod, tokenSize, reponseTime, dateCreated, agentId, insightId, sessionId, userId, userName);
	}
	
	public static void doRecordMessage(String messageId,
									   String messageType,
									   String messageData,
									   String messageMethod,
									   Integer tokenSize,
									   Double reponseTime,
									   ZonedDateTime dateCreated,
									   String agentId,
									   String insightId,
									   String sessionId,
									   String userId,
									   String userName) {
		
		// convert the time to UTC 
		ZonedDateTime dateCreatedUTC = Utility.convertZonedDateTimeToUTC(dateCreated);
		
		// boolean allowClob = modelInferenceLogsDb.getQueryUtil().allowClobJavaObject();
		String query = "INSERT INTO MESSAGE (MESSAGE_ID, MESSAGE_TYPE, MESSAGE_DATA, MESSAGE_METHOD, MESSAGE_TOKENS, RESPONSE_TIME,"
			+ " DATE_CREATED, AGENT_ID, INSIGHT_ID, SESSIONID, USER_ID, USER_NAME) " + 
			"	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, messageId);
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
				ps.setNull(index++, java.sql.Types.NULL);
			}
			ps.setDouble(index++, reponseTime);
			ps.setTimestamp(index++, java.sql.Timestamp.valueOf(dateCreatedUTC.toLocalDateTime()));
			ps.setString(index++, agentId);
			ps.setString(index++, insightId);
			ps.setString(index++, sessionId);
			ps.setString(index++, userId);
			ps.setString(index++, userName);
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
        try {
			UpdateQueryStruct qs = new UpdateQueryStruct();
			qs.setEngine(modelInferenceLogsDb);
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__INSIGHT_ID", "==", roomId));
			List<IQuerySelector> selectors = new ArrayList<>();
			List<Object> values = new ArrayList<>();
			selectors.add(new QueryColumnSelector("ROOM__IS_ACTIVE"));
			values.add(false);
			qs.setSelectors(selectors);
			qs.setValues(values);
			qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
			UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(qs);
			String updateQ = updateInterp.composeQuery();

            modelInferenceLogsDb.insertData(updateQ);
        } catch (Exception e) {
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
	
	public static boolean doSetNameForRoom(String userId, String roomId, String roomName) {
		Connection conn = connectToInferenceLogs();
        try {
        	UpdateQueryStruct qs = new UpdateQueryStruct();
            qs.setEngine(modelInferenceLogsDb);
            qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));
            qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__INSIGHT_ID", "==", roomId));
            List<IQuerySelector> selectors = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            selectors.add(new QueryColumnSelector("ROOM__ROOM_NAME"));
            values.add(roomName);
            qs.setSelectors(selectors);
            qs.setValues(values);
            qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
            UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(qs);
            String updateQ = updateInterp.composeQuery();
            
            modelInferenceLogsDb.insertData(updateQ);
        } catch (Exception e) {
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
		qs.addSelector(new QueryColumnSelector("FEEDBACK__RATING"));
		qs.addSelector(new QueryColumnSelector("FEEDBACK__FEEDBACK_TEXT"));

		qs.addRelation("MESSAGE__MESSAGE_ID", "FEEDBACK__MESSAGE_ID", "left.join");
		qs.addRelation("MESSAGE__MESSAGE_TYPE", "FEEDBACK__MESSAGE_TYPE", "left.join");
		
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
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__IS_ACTIVE", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_DATA", "!=", null));
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
		ModelInferenceLogsOwlCreator modelInfCreator = new ModelInferenceLogsOwlCreator(modelInferenceLogsDb);
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
			String tableName = tablePrimaryKeys.getValue0();
			Pair<List<String>, List<String>> primaryKeyInfo = tablePrimaryKeys.getValue1();
			List<String> primaryKeyNames = primaryKeyInfo.getValue0();
			List<String> primaryKeyTypes = primaryKeyInfo.getValue1();
			
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
			String tableName = tableForeignKeys.getValue0();
			Pair<List<String>, Pair<List<String>, List<String>>> foreignKeyInfo = tableForeignKeys.getValue1();
			List<String> tableColumns = foreignKeyInfo.getValue0();
			Pair<List<String>, List<String>> referenceDetails = foreignKeyInfo.getValue1();
			List<String> referenceTables = referenceDetails.getValue0();
			List<String> referenceColumns = referenceDetails.getValue1();
			
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
