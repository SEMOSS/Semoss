package prerna.engine.impl.model.inferencetracking;

import java.io.File;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import com.google.gson.Gson;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteEngineRunner;
import prerna.cluster.util.DeleteProjectRunner;
import prerna.date.SemossDate;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.project.api.IProject;
import prerna.project.impl.Project;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.query.querystruct.selectors.QueryTypedColumnSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.QueryExecutionUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class ModelInferenceLogsUtils {

  private static Logger classLogger = LogManager.getLogger(ModelInferenceLogsUtils.class);

  public static final String WORKSPACE_PROJECT_TAG = "Workspace_Project";
  public static final String WORKSPACE_DATABASE_TAG = "Workspace_Database";
  
  // Constants for Table
  private static final String MESSAGE_TABLE_NAME = "MESSAGE__";
  private static final String AGENT_TABLE_NAME = "AGENT__";
  private static final String ROOM_TABLE_NAME = "ROOM__";

  static IRDBMSEngine modelInferenceLogsDb;
  static boolean initialized = false;

  /** @throws Exception */
  public static void initModelInferenceLogsDatabase() throws Exception {
    modelInferenceLogsDb =
        (RDBMSNativeEngine) Utility.getDatabase(Constants.MODEL_INFERENCE_LOGS_DB);
    ModelInferenceLogsOwlCreator modelInfCreator =
        new ModelInferenceLogsOwlCreator(modelInferenceLogsDb);
    if (modelInfCreator.needsRemake()) {
      modelInfCreator.remakeOwl();
      // reset the local master metadata for model engine if we remade the OWL
      Utility.synchronizeEngineMetadata(Constants.MODEL_INFERENCE_LOGS_DB);
    }

    Connection conn = null;
    try {
      conn = modelInferenceLogsDb.makeConnection();
      executeInitModelInferenceDatabase(modelInferenceLogsDb, conn, modelInfCreator.getDBSchema());
      boolean primaryKeysAdded =
          addAllPrimaryKeys(modelInferenceLogsDb, conn, modelInfCreator.getDBPrimaryKeys());
      if (primaryKeysAdded) {
        addAllForeignKeys(modelInferenceLogsDb, conn, modelInfCreator.getDBForeignKeys());
      }

      if (!conn.getAutoCommit()) {
        conn.commit();
      }
    } finally {
      ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, conn, null, null);
    }
  }

  /**
   * @param engine
   * @param conn
   * @param columnNamesAndTypes
   * @throws SQLException
   */
  private static void executeInitModelInferenceDatabase(
      IRDBMSEngine engine, Connection conn, List<Pair<String, List<Pair<String, String>>>> dbSchema)
      throws SQLException {

    String database = engine.getDatabase();
    String schema = engine.getSchema();

    AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
    boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
    boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();

    boolean roomIdColumnWasAdded = false;
    boolean modelIdColumnWasAdded = false;

    for (Pair<String, List<Pair<String, String>>> tableSchema : dbSchema) {
      String tableName = tableSchema.getValue0();
      String[] colNames =
          tableSchema.getValue1().stream().map(Pair::getValue0).toArray(String[]::new);
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
          
          // was model id just added? 2025-06-26 addition. if so update w/ insight id
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
        migrateRoomAndMessageIds(conn);
    }
    
    // was modelId just added
    if (modelIdColumnWasAdded) {
    	migrateAgentAndModelIds(conn);
    }


    if (allowIfExistsIndexs) {
      String sql =
          queryUtil.createIndexIfNotExists("MESSAGE_INSIGHT_ID_INDEX", "MESSAGE", "INSIGHT_ID");
      executeSql(conn, sql);

      
      sql = queryUtil.createIndexIfNotExists("MESSAGE_ROOM_ID_INDEX", "MESSAGE", "ROOM_ID");
      executeSql(conn, sql);
  
      sql = queryUtil.createIndexIfNotExists("MESSAGE_USER_ID_INDEX", "MESSAGE", "USER_ID");
      executeSql(conn, sql);

      sql =
          queryUtil.createIndexIfNotExists("MESSAGE_DATE_CREATED_INDEX", "MESSAGE", "DATE_CREATED");
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

      if (!queryUtil.indexExists(
          engine, "MESSAGE_DATE_CREATED_INDEX", "MESSAGE", database, schema)) {
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
    }
  }

  /**
   * @param engine
   * @param conn
   * @param primaryKeys
   * @return
   */
  private static boolean addAllPrimaryKeys(
      IRDBMSEngine engine,
      Connection conn,
      List<Pair<String, Pair<List<String>, List<String>>>> primaryKeys) {
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
        String notNullQuery =
            "ALTER TABLE " + tableName + " ALTER COLUMN " + name + " " + type + ", ALTER COLUMN " + name + " SET NOT NULL;";
        try {
          executeSql(conn, notNullQuery);
        } catch (SQLException se) {
          classLogger.error(Constants.STACKTRACE, se);
          // We can't change it to NOT NULL so probably can't create the PRIMARY KEY
          return true;
        }
      }
      String primaryKeyConstraintName = tableName + "_KEY";
      if (queryUtil.allowIfExistsAddConstraint()) {
        String primaryKeyQuery =
            "ALTER TABLE "
                + tableName
                + " ADD CONSTRAINT IF NOT EXISTS "
                + primaryKeyConstraintName
                + " PRIMARY KEY ( "
                + String.join(",", primaryKeyNames)
                + " );";
        try {
          executeSql(conn, primaryKeyQuery);
        } catch (SQLException se) {
          classLogger.error(Constants.STACKTRACE, se);
        }
      } else {
        String primaryKeyQuery =
            "ALTER TABLE "
                + tableName
                + " ADD CONSTRAINT "
                + primaryKeyConstraintName
                + " PRIMARY KEY ( "
                + String.join(",", primaryKeyNames)
                + " );";
        try {
          if (!queryUtil.tableConstraintExists(
              conn,
              primaryKeyConstraintName,
              tableName,
              engine.getDatabase(),
              engine.getSchema())) {
            executeSql(conn, primaryKeyQuery);
          }
        } catch (SQLException se) {
          classLogger.error(Constants.STACKTRACE, se);
        }
      }
    }
    return true;
  }

  /**
   * @param engine
   * @param conn
   * @param foreignKeys
   */
  private static void addAllForeignKeys(
      IRDBMSEngine engine,
      Connection conn,
      List<Pair<String, Pair<List<String>, Pair<List<String>, List<String>>>>> foreignKeys) {
    ATTEMPT_TO__ADD_FOREIGN_KEY:
    for (Pair<String, Pair<List<String>, Pair<List<String>, List<String>>>> tableForeignKeys :
        foreignKeys) {
      String tableName = tableForeignKeys.getValue0();
      Pair<List<String>, Pair<List<String>, List<String>>> foreignKeyInfo =
          tableForeignKeys.getValue1();
      List<String> tableColumns = foreignKeyInfo.getValue0();
      Pair<List<String>, List<String>> referenceDetails = foreignKeyInfo.getValue1();
      List<String> referenceTables = referenceDetails.getValue0();
      List<String> referenceColumns = referenceDetails.getValue1();

      for (int i = 0; i < tableColumns.size(); i++) {
        String tableColumn = tableColumns.get(i);
        String refTable = referenceTables.get(i);
        String refColumn = referenceColumns.get(i);

        String constraintName =
            tableName + "_" + tableColumn + "_" + refTable + "_" + refColumn + "_KEY";
        constraintName = constraintName.replace(",", "");
        if (engine.getQueryUtil().allowIfExistsAddConstraint()) {
          String sqlStatement =
              String.format(
                  "ALTER TABLE %s ADD CONSTRAINT IF NOT EXISTS %s FOREIGN KEY (%s) REFERENCES %s (%s);",
                  tableName, constraintName, tableColumn, refTable, refColumn);
          try {
            executeSql(conn, sqlStatement);
          } catch (SQLException se) {
            classLogger.error(Constants.STACKTRACE, se);
            break ATTEMPT_TO__ADD_FOREIGN_KEY; // most likely incorrect syntax
          }
        } else {
          String sqlStatement =
              String.format(
                  "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s);",
                  tableName, constraintName, tableColumn, refTable, refColumn);
          try {
            if (!engine
                .getQueryUtil()
                .tableConstraintExists(
                    conn, constraintName, tableName, engine.getDatabase(), engine.getSchema())) {
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
  
  /**
   * @param conn
   * @throws SQLException
   */
  private static void migrateRoomAndMessageIds(Connection conn) {
	    try (Statement stmt = conn.createStatement()) {
	        int rCount = stmt.executeUpdate(
	            "UPDATE ROOM SET ROOM_ID = INSIGHT_ID WHERE ROOM_ID IS NULL OR ROOM_ID = ''"
	        );
	        int mCount = stmt.executeUpdate(
	            "UPDATE MESSAGE SET ROOM_ID = INSIGHT_ID WHERE ROOM_ID IS NULL OR ROOM_ID = ''"
	        );
	        classLogger.info("Room/Message room_id migration updated " + rCount + " ROOM rows and " + mCount + " MESSAGE rows.");
	    } catch (SQLException ex) {
	    	classLogger.error("Failed to migrate legacy ROOM_ID fields", ex);
	    }
	}
  
  /**
   * @param conn
   * @throws SQLException
   */
  private static void migrateAgentAndModelIds(Connection conn) {
	    try (Statement stmt = conn.createStatement()) {
	        int rCount = stmt.executeUpdate(
	            "UPDATE ROOM SET MODEL_ID = AGENT_ID WHERE MODEL_ID IS NULL OR MODEL_ID = ''"
	        );
	        int mCount = stmt.executeUpdate(
	            "UPDATE MESSAGE SET MODEL_ID = AGENT_ID WHERE MODEL_ID IS NULL OR MODEL_ID = ''"
	        );
	        classLogger.info("Room/Message model_id migration updated " + rCount + " ROOM rows and " + mCount + " MESSAGE rows.");
	    } catch (SQLException ex) {
	    	classLogger.error("Failed to migrate legacy AGENT_ID fields", ex);
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
   * @param userId
   * @param messageId
   * @return
   */
  public static boolean userIsMessageAuthor(String userId, String messageId) {
    SelectQueryStruct qs = new SelectQueryStruct();
    QueryFunctionSelector newSelector = new QueryFunctionSelector();
    newSelector.setAlias("Counts");
    newSelector.setFunction(QueryFunctionHelper.COUNT);
    newSelector.addInnerSelector(new QueryColumnSelector("MESSAGE__MESSAGE_ID"));

    qs.addSelector(newSelector);
    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_ID", "==", messageId));
    qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__USER_ID", "==", userId));
    IRawSelectWrapper wrapper = null;
    try {
      wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs);
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
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      if (wrapper != null) {
        try {
          wrapper.close();
        } catch (IOException e) {
          classLogger.error(Constants.STACKTRACE, e);
        }
      }
    }
    return false;
  }

  /**
   * @param messageId
   * @param feedbackText
   * @param rating
   */
  public static void recordFeedback(String messageId, String feedbackText, boolean rating) {
    if (feedbackExists(messageId)) {
      updateFeedback(messageId, feedbackText, rating);
    } else {
      insertFeedback(messageId, feedbackText, rating);
    }
  }

  /**
   * @param messageId
   * @return
   */
  public static boolean feedbackExists(String messageId) {
    SelectQueryStruct qs = new SelectQueryStruct();
    QueryFunctionSelector newSelector = new QueryFunctionSelector();
    newSelector.setAlias("Counts");
    newSelector.setFunction(QueryFunctionHelper.COUNT);
    newSelector.addInnerSelector(new QueryColumnSelector("FEEDBACK__MESSAGE_ID"));

    qs.addSelector(newSelector);
    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter("FEEDBACK__MESSAGE_ID", "==", messageId));
    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter("FEEDBACK__MESSAGE_TYPE", "==", "RESPONSE"));
    IRawSelectWrapper wrapper = null;
    try {
      wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs);
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
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      if (wrapper != null) {
        try {
          wrapper.close();
        } catch (IOException e) {
          classLogger.error(Constants.STACKTRACE, e);
        }
      }
    }
    return false;
  }

  /**
   * @param messageId
   * @param feedbackText
   * @param rating
   */
  public static void insertFeedback(String messageId, String feedbackText, boolean rating) {
    String query =
        "INSERT INTO FEEDBACK (MESSAGE_ID, MESSAGE_TYPE, FEEDBACK_TEXT, FEEDBACK_DATE, RATING) "
            + "VALUES (?, ?, ?, ?, ?)";
    PreparedStatement ps = null;
    try {
      ps = modelInferenceLogsDb.getPreparedStatement(query);
      int index = 1;
      ps.setString(index++, messageId);
      ps.setString(index++, "RESPONSE");
      ps.setString(index++, feedbackText);
      ps.setTimestamp(index++, Utility.getCurrentSqlTimestampUTC());
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

  /**
   * @param messageId
   * @param feedbackText
   * @param rating
   */
  public static void updateFeedback(String messageId, String feedbackText, boolean rating) {
    try {
      UpdateQueryStruct qs = new UpdateQueryStruct();
      qs.setEngine(modelInferenceLogsDb);
      qs.addExplicitFilter(
          SimpleQueryFilter.makeColToValFilter("FEEDBACK__MESSAGE_ID", "==", messageId));
      qs.addExplicitFilter(
          SimpleQueryFilter.makeColToValFilter("FEEDBACK__MESSAGE_TYPE", "==", "RESPONSE"));
      List<IQuerySelector> selectors =
          new ArrayList<>(
              Arrays.asList(
                  new QueryColumnSelector("FEEDBACK__FEEDBACK_TEXT"),
                  new QueryColumnSelector("FEEDBACK__FEEDBACK_DATE"),
                  new QueryColumnSelector("FEEDBACK__RATING")));

      List<Object> values =
          new ArrayList<>(
              Arrays.asList(
                  feedbackText, new SemossDate(Utility.getCurrentZonedDateTimeUTC()), rating));

      qs.setSelectors(selectors);
      qs.setValues(values);
      qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
      UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(qs);
      String updateQ = updateInterp.composeQuery();

      modelInferenceLogsDb.insertData(updateQ);
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
    }
  }

  /** USAGE HELPER FUNCTIONS */

  /**
   * Function returns the number of unique calls (Inputs) per a model
   *
   * @param engineId
   * @param offset
   * @param limit
   * @param dateFilter
   * @return
   */
  public static List<Map<String, Object>> getOverAllEngineUsageFromModelInferenceLogs(
      String engineId, String limit, String offset, String startDate, String endDate) {
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
   * Returns a list of total tokens used per project for engineId passed in
   *
   * @param engineId
   * @param dateFilter
   * @param offset
   * @param limit
   * @return
   */
  public static List<Map<String, Object>> getTokenUsagePerProjectForEngine(
      String engineId, String limit, String offset, String startDate, String endDate) {
    SelectQueryStruct qs = new SelectQueryStruct();
    qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_NAME"));

    QueryFunctionSelector sumTokenSelector = new QueryFunctionSelector();
    sumTokenSelector.setAlias("TOTAL_NUMBER_OF_TOKENS");
    sumTokenSelector.setFunction(QueryFunctionHelper.SUM);
    sumTokenSelector.addInnerSelector(
        new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_TOKENS"));
    qs.addSelector(sumTokenSelector);

    QueryFunctionSelector countNumberRequestSelector = new QueryFunctionSelector();
    countNumberRequestSelector.setAlias("TOTAL_NUMBER_OF_REQUEST");
    countNumberRequestSelector.setFunction(QueryFunctionHelper.COUNT);
    countNumberRequestSelector.addInnerSelector(
        new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_ID"));
    qs.addSelector(countNumberRequestSelector);

    qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "PROJECT_ID"));
    qs.addRelation(MESSAGE_TABLE_NAME + "AGENT_ID", ROOM_TABLE_NAME + "AGENT_ID", "left.join");
    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "AGENT_ID", "==", engineId));
    addStartDateEndDateFitler(qs, startDate, endDate);

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
   * @param engineId
   * @param limit
   * @param offset
   * @param startDate
   * @param endDate
   * @return
   */
  public static List<Map<String, Object>> getUserUsagePerEngine(
      String engineId, String limit, String offset, String startDate, String endDate) {
    SelectQueryStruct qs = new SelectQueryStruct();
    qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_NAME"));
    qs.addSelector(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_ID"));

    QueryFunctionSelector sumTokenSelector = new QueryFunctionSelector();
    sumTokenSelector.setAlias("TOTAL_NUMBER_OF_TOKENS");
    sumTokenSelector.setFunction(QueryFunctionHelper.SUM);
    sumTokenSelector.addInnerSelector(
        new QueryColumnSelector(MESSAGE_TABLE_NAME + "MESSAGE_TOKENS"));
    qs.addSelector(sumTokenSelector);

    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "AGENT_ID", "==", engineId));
    addStartDateEndDateFitler(qs, startDate, endDate);

    addLimitAndOffSet(qs, limit, offset);
    qs.addGroupBy(new QueryColumnSelector(MESSAGE_TABLE_NAME + "USER_NAME"));

    return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
  }

  /**
   * @param qs
   * @param startDate
   * @param endDate
   */
  private static void addStartDateEndDateFitler(
      SelectQueryStruct qs, String startDate, String endDate) {
    if ((startDate != null && !startDate.trim().isEmpty())
        && (endDate != null && !endDate.trim().isEmpty())) {
      AndQueryFilter andFilters = new AndQueryFilter();
      andFilters.addFilter(
          SimpleQueryFilter.makeColToValFilter(
              MESSAGE_TABLE_NAME + "DATE_CREATED", ">=", startDate));
      andFilters.addFilter(
          SimpleQueryFilter.makeColToValFilter(MESSAGE_TABLE_NAME + "DATE_CREATED", "<=", endDate));
      qs.addExplicitFilter(andFilters);
    }
  }

  /**
   * @param projectId
   * @return
   */
  public static Map<String, Object> getProjectUsageFromModelInferenceLogs(String projectId) {
    // First get a list of roomIds from Room
    List<String> roomIdList = getRoomIdListPerProject(projectId);
    // Second query against message to find number of unique calls? Not sure what we are tracking
    // from projects just yet
    SelectQueryStruct qs = new SelectQueryStruct();
    QueryFunctionSelector newSelector = new QueryFunctionSelector();
    newSelector.setAlias("Unique_Calls");
    newSelector.setFunction(QueryFunctionHelper.COUNT);
    newSelector.addInnerSelector(new QueryColumnSelector("MESSAGE__MESSAGE_ID"));

    qs.addSelector(newSelector);
    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter("MESSAGE__ROOM_ID", "==", roomIdList));
    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_TYPE", "==", "INPUT"));
    return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs).get(0);
  }

  /**
   * @param projectId
   * @return
   */
  public static List<String> getRoomIdListPerProject(String projectId) {
    SelectQueryStruct qs = new SelectQueryStruct();
    qs.addSelector(new QueryColumnSelector("ROOM__ROOM_ID"));
    qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__PROJECT_ID", "==", projectId));
    List<String> insightIdList = QueryExecutionUtility.flushToListString(modelInferenceLogsDb, qs);
    return insightIdList;
  }

  /** @param user */
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

  /**
   * @param roomName
   * @param roomContext
   * @param userId
   * @param userName
   * @param userEmail
   * @param agentType
   * @param agentId
   * @param isActive
   * @param projectId
   * @param projectName
   * @return
   */
  public static String doCreateNewConversation(
      String roomName,
      String roomContext,
      String userId,
      String userName,
      String userEmail,
      String agentType,
      String agentId,
      Boolean isActive,
      String projectId,
      String projectName) {
    String convoId = UUID.randomUUID().toString();
    doCreateNewConversation(
        convoId,
        roomName,
        roomContext,
        userId,
        userName,
        userEmail,
        agentType,
        agentId,
        isActive,
        projectId,
        projectName);
    return convoId;
  }

  /**
   * @param insightId
   * @param roomName
   * @param roomContext
   * @param userId
   * @param userName
   * @param userEmail
   * @param agentType
   * @param agentId
   * @param isActive
   * @param projectId
   * @param projectName
   */
  public static void doCreateNewConversation(
      String insightId,
      String roomName,
      String roomContext,
      String userId,
      String userName,
      String userEmail,
      String agentType,
      String agentId,
      Boolean isActive,
      String projectId,
      String projectName) {
	  
	   doCreateNewConversation(insightId,insightId, roomName, roomContext, userId, userName, userEmail, agentType, agentId, isActive, projectId, projectName, null);
  }
   
  
  /**
   * @param insightId
   * @param roomId
   * @param roomName
   * @param roomContext
   * @param userId
   * @param userName
   * @param userEmail
   * @param agentType
   * @param agentId
   * @param isActive
   * @param projectId
   * @param projectName
   * @param options
   */
  public static void doCreateNewConversation(
      String insightId,
      String roomId,
      String roomName,
      String roomContext,
      String userId,
      String userName,
      String userEmail,
      String agentType,
      String agentId,
      Boolean isActive,
      String projectId,
      String projectName,
      Map<String, Object> options) {
    String query =
        "INSERT INTO ROOM (INSIGHT_ID, ROOM_ID, ROOM_NAME, "
            + "ROOM_CONTEXT, USER_ID, USER_NAME, USER_EMAIL_ID, "
            + "AGENT_TYPE, AGENT_ID, IS_ACTIVE, "
            + "DATE_CREATED, PROJECT_ID, PROJECT_NAME, OPTIONS) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    // boolean allowClob = modelInferenceLogsDb.getQueryUtil().allowClobJavaObject();
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
        modelInferenceLogsDb
            .getQueryUtil()
            .handleInsertionOfClob(ps.getConnection(), ps, roomContext, index++, new Gson());
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
      if (options != null) {
          modelInferenceLogsDb
              .getQueryUtil()
              .handleInsertionOfClob(ps.getConnection(), ps, options, index++, new Gson());
        } else {
          ps.setNull(index++, java.sql.Types.NULL);
        }
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

  /**
   * @param roomId
   * @return
   */
  public static boolean doCheckRoomExists(String roomId) {
    String query = "SELECT COUNT(*) FROM ROOM WHERE ROOM_ID = ?";
    PreparedStatement ps = null;
    try {
      ps = modelInferenceLogsDb.getPreparedStatement(query);
      int index = 1;
      ps.setString(index++, roomId);
      ps.execute();
      if (ps.execute()) {
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

  /**
   * @param agentId
   * @return
   */
  public static boolean doModelIsRegistered(String agentId) {
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
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
    }
    return false;
  }

  /**
   * @param agentName
   * @param agentDescription
   * @param agentType
   * @param author
   * @return
   */
  public static String doCreateNewAgent(
      String agentName, String agentDescription, String agentType, String author) {
    String agentId = UUID.randomUUID().toString();
    doCreateNewAgent(agentId, agentName, agentDescription, agentType, author);
    return agentId;
  }

  /**
   * @param agentId
   * @param agentName
   * @param agentDescription
   * @param agentType
   * @param author
   */
  public static void doCreateNewAgent(
      String agentId, String agentName, String agentDescription, String agentType, String author) {
    String query =
        "INSERT INTO AGENT (AGENT_ID, AGENT_NAME, DESCRIPTION, AGENT_TYPE, "
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
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
    }
  }

  /**
   * @param messageId
   * @param messageType
   * @param messageData
   * @param messageMethod
   * @param tokenSize
   * @param reponseTime
   * @param agentId
   * @param insightId
   * @param sessionId
   * @param userId
   * @param userName
   */
  public static void doRecordMessage(
      String messageId,
      String messageType,
      String messageData,
      String messageMethod,
      Integer tokenSize,
      Double reponseTime,
      String agentId,
      String insightId,
      String sessionId,
      String userId,
      String userName,
      String userEmail) {
    ZonedDateTime dateCreated = ZonedDateTime.now();
    doRecordMessage(
        messageId,
        messageType,
        messageData,
        messageMethod,
        tokenSize,
        reponseTime,
        dateCreated,
        agentId,
        insightId,
        sessionId,
        insightId, //roomId
        userId,
        userName,
        userEmail);
  }

  /**
   * @param messageId
   * @param messageType
   * @param messageData
   * @param messageMethod
   * @param tokenSize
   * @param reponseTime
   * @param dateCreated
   * @param agentId
   * @param insightId
   * @param sessionId
   * @param roomId
   * @param userId
   * @param userName
   * @param userEmail
   */
  public static void doRecordMessage(
      String messageId,
      String messageType,
      String messageData,
      String messageMethod,
      Integer tokenSize,
      Double reponseTime,
      ZonedDateTime dateCreated,
      String agentId,
      String insightId,
      String sessionId,
      String roomId,
      String userId,
      String userName,
      String userEmail) {
    // convert the time to UTC
    ZonedDateTime dateCreatedUTC = Utility.convertZonedDateTimeToUTC(dateCreated);

    // boolean allowClob = modelInferenceLogsDb.getQueryUtil().allowClobJavaObject();
    String query =
        "INSERT INTO MESSAGE (MESSAGE_ID, MESSAGE_TYPE, MESSAGE_DATA, MESSAGE_METHOD, MESSAGE_TOKENS, RESPONSE_TIME,"
            + " DATE_CREATED, AGENT_ID, INSIGHT_ID, ROOM_ID, SESSIONID, USER_ID, USER_NAME, USER_EMAIL_ID) "
            + "	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    PreparedStatement ps = null;
    try {
      ps = modelInferenceLogsDb.getPreparedStatement(query);
      int index = 1;
      ps.setString(index++, messageId);
      ps.setString(index++, messageType);
      if (messageData != null) {
        modelInferenceLogsDb
            .getQueryUtil()
            .handleInsertionOfBlob(ps.getConnection(), ps, messageData, index++);
      } else {
        ps.setNull(index++, java.sql.Types.NULL);
      }
      ps.setString(index++, messageMethod);
      if (tokenSize != null) {
        ps.setInt(index++, tokenSize);
      } else {
        ps.setNull(index++, java.sql.Types.INTEGER);
      }
      ps.setDouble(index++, reponseTime);
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
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
    }
  }

  /**
   * @param userId
   * @param roomId
   * @return
   */
  public static boolean doSetRoomToInactive(String userId, String roomId) {
    try {
      UpdateQueryStruct qs = new UpdateQueryStruct();
      qs.setEngine(modelInferenceLogsDb);
      qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));
      qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__ROOM_ID", "==", roomId));
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
    }
    return true;
  }

  /**
   * @param userId
   * @param roomId
   * @param roomName
   * @return
   */
  public static boolean doSetNameForRoom(String userId, String roomId, String roomName) {
    try {
      UpdateQueryStruct qs = new UpdateQueryStruct();
      qs.setEngine(modelInferenceLogsDb);
      qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));
      qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__ROOM_ID", "==", roomId));
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
    }
    return true;
  }

  /**
   * @param userId
   * @param roomId
   * @param dateSort
   * @return
   */
  public static List<Map<String, Object>> doRetrieveConversation(
      String userId, String roomId, String dateSort) {
    SelectQueryStruct qs = retrieveMessageQS(userId, roomId, dateSort);
    return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
  }

  /**
   * @param userId
   * @param roomId
   * @param dateSort
   * @param limit
   * @param offset
   * @return
   */
  public static List<Map<String, Object>> doRetrieveConversation(
      String userId, String roomId, String dateSort, Integer limit, Integer offset) {
    SelectQueryStruct qs = retrieveMessageQS(userId, roomId, dateSort);
    qs.setLimit(limit);
    qs.setOffSet(offset);
    List<Map<String, Object>> response =
        QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
    if (dateSort.equals("DESC")) {
      Collections.reverse(response);
    }
    return response;
  }

  /**
   * @param userId
   * @param roomId
   * @param dateSort
   * @return
   */
  private static SelectQueryStruct retrieveMessageQS(
      String userId, String roomId, String dateSort) {
    SelectQueryStruct qs = new SelectQueryStruct();
    qs.addSelector(new QueryColumnSelector("MESSAGE__DATE_CREATED"));
    qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_TYPE"));
    qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_DATA"));
    qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_ID"));
    qs.addSelector(new QueryColumnSelector("FEEDBACK__RATING"));
    qs.addSelector(new QueryColumnSelector("FEEDBACK__FEEDBACK_TEXT"));

    qs.addRelation("MESSAGE__MESSAGE_ID", "FEEDBACK__MESSAGE_ID", "left.join");
    qs.addRelation("MESSAGE__MESSAGE_TYPE", "FEEDBACK__MESSAGE_TYPE", "left.join");

    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter("MESSAGE__ROOM_ID", "==", roomId));
    qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__USER_ID", "==", userId));
    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_METHOD", "==", "ask"));
    qs.addOrderBy(new QueryColumnOrderBySelector("MESSAGE__DATE_CREATED", dateSort));
    return qs;
  }

  /**
   * @param userId
   * @param roomId
   * @param dateSort
   * @return
   */
  public static List<Map<String, Object>> doRetrieveNearestNeighbor(
      String userId, String roomId, String dateSort) {
    SelectQueryStruct qs = new SelectQueryStruct();
    qs.addSelector(new QueryColumnSelector("MESSAGE__DATE_CREATED"));
    qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_TYPE"));
    qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_DATA"));
    qs.addSelector(new QueryColumnSelector("MESSAGE__MESSAGE_ID"));
    qs.addSelector(new QueryColumnSelector("FEEDBACK__RATING"));
    qs.addSelector(new QueryColumnSelector("FEEDBACK__FEEDBACK_TEXT"));

    qs.addRelation("MESSAGE__MESSAGE_ID", "FEEDBACK__MESSAGE_ID", "left.join");
    qs.addRelation("MESSAGE__MESSAGE_TYPE", "FEEDBACK__MESSAGE_TYPE", "left.join");

    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter("MESSAGE__ROOM_ID", "==", roomId));
    qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MESSAGE__USER_ID", "==", userId));
    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_METHOD", "==", "nearestNeighbor"));
    qs.addOrderBy(new QueryColumnOrderBySelector("MESSAGE__DATE_CREATED", dateSort));
    return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
  }

  /**
   * @param userId
   * @param roomId
   * @return
   */
  public static List<Map<String, Object>> doVerifyConversation(String userId, String roomId) {
    SelectQueryStruct qs = new SelectQueryStruct();
    qs.addSelector(new QueryColumnSelector("ROOM__ROOM_ID"));
    qs.addSelector(new QueryColumnSelector("ROOM__PROJECT_ID"));

    qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__ROOM_ID", "==", roomId));
    qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));
    qs.setDistinct(true);
    return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
  }

  /**
   * @param userId
   * @param projectId
   * @return
   */
  public static List<Map<String, Object>> getUserConversations(String userId, String projectId) {
    SelectQueryStruct qs = new SelectQueryStruct();
    qs.addSelector(new QueryColumnSelector("ROOM__ROOM_ID"));
    qs.addSelector(new QueryColumnSelector("ROOM__ROOM_NAME"));
    qs.addSelector(new QueryColumnSelector("ROOM__ROOM_CONTEXT"));
    qs.addSelector(new QueryColumnSelector("ROOM__AGENT_ID", "MODEL_ID"));
    qs.addSelector(new QueryColumnSelector("ROOM__DATE_CREATED"));

    SelectQueryStruct subQs = new SelectQueryStruct();
    subQs.addSelector(new QueryColumnSelector("ROOM__ROOM_ID"));
    subQs.addRelation("ROOM__ROOM_ID", "MESSAGE__ROOM_ID", "inner.join");
    subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));
    subQs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter("ROOM__IS_ACTIVE", "==", true, PixelDataType.BOOLEAN));
    subQs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_DATA", "!=", null));
    if (projectId != null) {
      subQs.addExplicitFilter(
          SimpleQueryFilter.makeColToValFilter("ROOM__PROJECT_ID", "==", projectId));
    }
    qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ROOM__ROOM_ID", "IN", subQs));

    qs.addOrderBy(new QueryColumnOrderBySelector("ROOM__DATE_CREATED", "DESC"));
    return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
  }

  /** @param messageId */
  public static void removeFeedback(String messageId) {
    if (!feedbackExists(messageId)) {
      throw new SemossPixelException("No feedback found for the given messageId to remove.");
    }
    deleteFeedbackEntry(messageId);
  }

  /**
   * @param userId
   * @param roomId
   * @return
   */
  public static List<Map<String,Object>> getRoomContext(String userId, String roomId) {
    SelectQueryStruct qs = new SelectQueryStruct();
    qs.addSelector(new QueryColumnSelector(ROOM_TABLE_NAME + "ROOM_CONTEXT"));
    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter(ROOM_TABLE_NAME + "ROOM_ID", "==", roomId));
    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter(ROOM_TABLE_NAME + "USER_ID", "==", userId));

    return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
  }

  /**
   * @param userId
   * @param roomId
   * @return
   */
  public static Blob getRoomOptions(String roomId, String userId) {
	String query = "SELECT OPTIONS FROM ROOM WHERE ROOM_ID = ? AND USER_ID = ?";
	PreparedStatement ps = null;
	try {
		ps = modelInferenceLogsDb.getPreparedStatement(query);
		ps.setString(1, roomId);
		ps.setString(2, userId);
		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
			return rs.getBlob("OPTIONS");
		}
	} catch (Exception e) {
	    classLogger.error(Constants.STACKTRACE, e);
	} finally {
	    ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
	}
	
	return null;
  }

  /**
   * @param userId
   * @param roomId
   * @param options
   * @return
   */
  public static void setRoomOptions(String roomId, String userId, Map<String, Object> options) {
    String query =
        "UPDATE ROOM SET OPTIONS = ? WHERE USER_ID = ? AND ROOM_ID = ?";

    PreparedStatement ps = null;
    try {
      ps = modelInferenceLogsDb.getPreparedStatement(query);
      int index = 1;
      if (options != null) {
          modelInferenceLogsDb
              .getQueryUtil()
              .handleInsertionOfClob(ps.getConnection(), ps, options, index++, new Gson());
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
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
    }
  }

  /**
   * @param userId
   * @param roomId
   * @param context
   * @return
   */
  public static void setRoomContext(String roomId, String userId, String context) {
    try {
      UpdateQueryStruct qs = new UpdateQueryStruct();
      qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
      qs.setEngine(modelInferenceLogsDb);

      qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userId));
      qs.addExplicitFilter(
          SimpleQueryFilter.makeColToValFilter("ROOM__ROOM_ID", "==", roomId)); //TODO: this will break for legacy rooms
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
    }
  }

  /** @param messageId */
  private static void deleteFeedbackEntry(String messageId) {
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
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
    }
  }

  /**
   * @param restrictionMode
   * @param user
   * @param currentDateTime
   * @param frequency
   * @return
   */
  public static Number getTotalTokensOrTotalResponseTime(
      String restrictionMode,
      User user,
      String engineId,
      ZonedDateTime currentDateTime,
      String frequency) {
    if (restrictionMode == null) {
      throw new IllegalArgumentException("Must pass in a valid restriction mode");
    }

    // Initialize the date range map (start and end dates)
    Map<String, ZonedDateTime> dates = new HashMap<>();
    // Determine the start and end date based on the given frequency
    if (frequency.equalsIgnoreCase("WEEK")) {
      dates = Utility.getWeekStartEndDate(currentDateTime);
    } else if (frequency.equalsIgnoreCase("MONTH")) {
      // Get start and end date for the current month
      dates = Utility.getMonthStartEndDate(currentDateTime);
    } else {
      // assume they want daily
      dates.put("start", Utility.getCurrentZonedDateTimeUTC());
      dates.put("end", Utility.getCurrentZonedDateTimeUTC());
    }

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
    String query =
        "SELECT "
            + sumColumn
            + " AS \"current_usage\" FROM MESSAGE WHERE USER_ID=? AND AGENT_ID=? AND DATE_CREATED BETWEEN ? AND ?";
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = modelInferenceLogsDb.getPreparedStatement(query);
      int psIndex = 1;
      ps.setString(psIndex++, user.getAccessToken(user.getLogins().get(0)).getId());
      ps.setString(psIndex++, engineId);
      ps.setDate(psIndex++, java.sql.Date.valueOf(startDate.toLocalDate()));
      ps.setDate(psIndex++, java.sql.Date.valueOf(endDate.toLocalDate()));

      RawRDBMSSelectWrapper wrapper =
          RawRDBMSSelectWrapper.directExecutionPreparedStatement(
              modelInferenceLogsDb, ps.getConnection(), ps, query, false);

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
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, rs);
    }
    return null;
  }

  /**
   * @param restrictionMode
   * @param user
   * @param engineId
   * @param currentDateTime
   * @param frequency
   * @return
   */
  public static Number getTotalUsageForUser(
      String restrictionMode,
      User user,
      String engineId,
      ZonedDateTime currentDateTime,
      String frequency) {
    if (restrictionMode == null) {
      throw new IllegalArgumentException("Must pass in a valid restriction mode");
    }

    // Step 1: Get the list of Engine IDs with MAXRESPONSETIME or MAXTOKENS for the specific user
    List<String> engineIdExcludeList =
        SecurityEngineUtils.getModelEngineIdsWithRestrictions(user, engineId);
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

    // Step 2: Get the date range based on the frequency
    // Initialize the date range map (start and end dates)
    Map<String, ZonedDateTime> dates = new HashMap<>();
    // Determine the start and end date based on the given frequency
    if (frequency.equals("WEEK")) {
      dates = Utility.getWeekStartEndDate(currentDateTime);
    } else if (frequency.equals("MONTH")) {
      // Get start and end date for the current month
      dates = Utility.getMonthStartEndDate(currentDateTime);
    } else {
      dates.put("start", Utility.getCurrentZonedDateTimeUTC());
      dates.put("end", Utility.getCurrentZonedDateTimeUTC());
    }
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
    String query =
        "SELECT "
            + sumColumn
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

      RawRDBMSSelectWrapper wrapper =
          RawRDBMSSelectWrapper.directExecutionPreparedStatement(
              modelInferenceLogsDb, ps.getConnection(), ps, query, false);

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
      classLogger.error(Constants.STACKTRACE, e);
    } finally {
      ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, rs);
    }

    return null;
  }

  /* -------- ROOM PIECES -------*/

  /**
   * @param roomId
   * @param userId
   * @param messageHistory
   */
  public static boolean llm2_updateRoomMessages(
      String roomId, String userId, String messageHistory) {
    PreparedStatement updateStmt = null;
    try {
      // Update messages and timestamp where room and user match
      String query =
          "UPDATE ROOM SET MESSAGES = ?, UPDATED_AT = ? WHERE ROOM_ID = ? AND USER_ID = ?";
      updateStmt = modelInferenceLogsDb.getPreparedStatement(query);

      // Prepare statement
      updateStmt.setString(1, messageHistory);
      updateStmt.setTimestamp(2, java.sql.Timestamp.valueOf(LocalDateTime.now()));
      updateStmt.setString(3, roomId);
      updateStmt.setString(4, userId);

      // Execute update
      int rows = updateStmt.executeUpdate();
      if (!updateStmt.getConnection().getAutoCommit()) {
        updateStmt.getConnection().commit();
      }
      return rows > 0;

    } catch (Exception e) {
      classLogger.error("Error updating room messages: ", e);
      throw new IllegalArgumentException("Error updating room messages: " + e.getMessage());
    } finally {
      ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, updateStmt, null);
    }
  }

  public static Room getRoomById(String room_id, String user_id) {
    String query = "SELECT *  " + "FROM ROOM WHERE ROOM_ID = ? and USER_ID = ? ";
    PreparedStatement stmt = null;
    ResultSet resultSet = null;
    try {
      stmt = modelInferenceLogsDb.getPreparedStatement(query);
      stmt.setString(1, room_id);
      stmt.setString(2, user_id);
      resultSet = stmt.executeQuery();
      if (resultSet.next()) {
        return new Room(
            resultSet.getString("ROOM_ID"),
            resultSet.getString("USER_ID"),
            resultSet.getString("ROOM_NAME"),
            resultSet.getString("ROOM_CONTEXT"),
            resultSet.getString("SHARE_ID"),
            resultSet.getBoolean("IS_ACTIVE"),
            resultSet.getTimestamp("DATE_CREATED"),
            resultSet.getTimestamp("UPDATED_AT"),
            resultSet.getString("MESSAGES"),
            resultSet.getBoolean("PINNED"),
            resultSet.getString("OPTIONS"),
            resultSet.getString("MODEL_ID"));
      } else {
        return null;
      }

    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Get a list of the user's active rooms
   *
   * @param db - playground database
   * @param userId - user accessing the db
   * @return a list of the users room
   */
  public static List<Map<String, Object>> getUserActiveRooms(String roomId, String userId) {
    SelectQueryStruct qs = new SelectQueryStruct();
    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter(
            ROOM_TABLE_NAME + "IS_ACTIVE", "==", true, PixelDataType.BOOLEAN));
    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter(ROOM_TABLE_NAME + "ROOM_ID", "==", roomId));
    qs.addExplicitFilter(
        SimpleQueryFilter.makeColToValFilter(ROOM_TABLE_NAME + "USER_ID", "==", userId));

    return QueryExecutionUtility.flushRsToMap(modelInferenceLogsDb, qs);
  }

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

  
  
  
  /* -------- WORKSPACE PIECES -------*/
  
  
	/**
	 * 
	 * @param workspaceId
	 * @param ownerId
	 * @param workspaceName
	 * @param workspaceDescription
	 * @param systemPrompt
	 * @param sharingEnabled
	 */
	public static void createNewWorkspaceEntry(String workspaceId, String ownerId, String workspaceName,
			String workspaceDescription, String systemPrompt, boolean sharingEnabled) throws Exception {
		Gson gson = new Gson();
		Timestamp now = Utility.getCurrentSqlTimestampUTC();

		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try(
				PreparedStatement ps = con.prepareStatement("INSERT INTO WORKSPACE (WORKSPACE_ID, NAME, DESCRIPTION, SYSTEM_PROMPT, OWNER, SHARING_ENABLED, DATE_CREATED, DATE_UPDATED) VALUES (?,?,?,?,?,?,?,?)")
			) {
				int index = 1;
				ps.setString(index++, workspaceId);
				ps.setString(index++, workspaceName);
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(con, ps, workspaceDescription,
						index++, gson);
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(con, ps, systemPrompt, index++,
						gson);
				ps.setString(index++, ownerId);
				ps.setBoolean(index++, sharingEnabled);;
				ps.setTimestamp(index++, now);
				ps.setTimestamp(index++, now);
				ps.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error creating workspace: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}
	
	/**
	 * 
	 * @param workspaceId
	 * @param workspaceName
	 * @param workspaceDescription
	 * @param systemPrompt
	 * @param sharingEnabled
	 */
	public static void updateWorkspaceEntry(String workspaceId, String workspaceName,
			String workspaceDescription, String systemPrompt, boolean sharingEnabled) throws Exception {
		Gson gson = new Gson();
		Timestamp now = Utility.getCurrentSqlTimestampUTC();

		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try(
				PreparedStatement ps = con.prepareStatement("UPDATE WORKSPACE SET NAME = ?, DESCRIPTION = ?, SYSTEM_PROMPT = ?, SHARING_ENABLED = ?, DATE_UPDATED = ? WHERE WORKSPACE_ID = ?")
			) {
				int index = 1;
				ps.setString(index++, workspaceName);
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(con, ps, workspaceDescription,
						index++, gson);
				modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(con, ps, systemPrompt, index++,
						gson);
				ps.setBoolean(index++, sharingEnabled);
				ps.setTimestamp(index++, now);
				ps.setString(index++, workspaceId);
				ps.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error updating workspace: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}

	/**
	 * 
	 * @param workspaceId
	 */
	public static void deleteWorkspaceEntry(String workspaceId) {
		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try(
				PreparedStatement ps1 = con.prepareStatement("DELETE FROM WORKSPACE_KNOWLEDGE WHERE WORKSPACE_ID = ?");
				PreparedStatement ps2 = con.prepareStatement("UPDATE ROOM SET WORKSPACE_ID = NULL WHERE WORKSPACE_ID = ?");
				PreparedStatement ps3 = con.prepareStatement("DELETE FROM WORKSPACE WHERE WORKSPACE_ID = ?");
			) {
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
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error deleting workspace: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}
	
	/**
	 * 
	 * @param workspaceId
	 */
	public static Map<String, Object> getWorkspaceEntry(String workspaceId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("WORKSPACE__WORKSPACE_ID", "workspace_id"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE__DESCRIPTION", "description"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE__SYSTEM_PROMPT", "system_prompt"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE__OWNER", "owner"));
		
		qs.addSelector(new QueryColumnSelector("WORKSPACE__SHARING_ENABLED", "sharing_enabled"));
		
		QueryFunctionSelector createdSelector = QueryFunctionSelector.makeFunctionSelector("TO_CHAR", "WORKSPACE__DATE_CREATED", "date_created");
		createdSelector.addAdditionalParam(new String[] {"'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'"});
		qs.addSelector(createdSelector);
		
		QueryFunctionSelector updatedSelector = QueryFunctionSelector.makeFunctionSelector("TO_CHAR", "WORKSPACE__DATE_UPDATED", "date_updated");
		updatedSelector.addAdditionalParam(new String[] {"'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'"});
		qs.addSelector(updatedSelector);
		
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("WORKSPACE__WORKSPACE_ID", "==", workspaceId));
		
		qs.setLimit(1L);
		
		Map<String, Object> result = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs)) {
			while(wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				result = new HashMap<String, Object>();
				for(int i = 0; i < headers.length; i++) {
					if(values[i] instanceof java.sql.Clob) {
						String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
						result.put(headers[i], value);
					} else if(values[i] instanceof java.sql.Blob) {
						String value = AbstractSqlQueryUtil.flushBlobToString((java.sql.Blob) values[i]);
						result.put(headers[i], value);
					} else {
						result.put(headers[i], values[i]);
					}
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return result;
	}
	
	/**
	 * 
	 * @param workspaceId
	 * @param user
	 * @param limit
	 * @param offset
	 * @param filters
	 * @param sorts
	 * @param sharedWorkspaceIds
	 */
	public static Map<String, Object> getWorkspaceRoomsForUser(String workspaceId, User user, long limit, long offset, GenRowFilters filters, List<IQuerySort> sorts) {
		Collection<String> userIds = getUserFiltersQs(user);
		
		SelectQueryStruct qs = new SelectQueryStruct();
	    qs.addSelector(new QueryColumnSelector("ROOM__ROOM_ID", "room_id"));
	    qs.addSelector(new QueryColumnSelector("ROOM__ROOM_NAME", "room_name"));
	    qs.addSelector(new QueryColumnSelector("ROOM__ROOM_CONTEXT", "room_context"));
	    qs.addSelector(new QueryColumnSelector("ROOM__AGENT_ID", "model_id"));
	    qs.addSelector(new QueryColumnSelector("ROOM__WORKSPACE_ID", "workspace_id"));
	    
	    QueryFunctionSelector createdSelector = QueryFunctionSelector.makeFunctionSelector("TO_CHAR", "ROOM__DATE_CREATED", "date_created");
		createdSelector.addAdditionalParam(new String[] {"'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'"});
		qs.addSelector(createdSelector);
		
		QueryFunctionSelector updatedSelector = QueryFunctionSelector.makeFunctionSelector("TO_CHAR", "ROOM__UPDATED_AT", "date_updated");
		updatedSelector.addAdditionalParam(new String[] {"'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'"});
		qs.addSelector(updatedSelector);

	    SelectQueryStruct subQs = new SelectQueryStruct();
	    subQs.addSelector(new QueryColumnSelector("ROOM__ROOM_ID"));
	    subQs.addRelation("ROOM__ROOM_ID", "MESSAGE__ROOM_ID", "inner.join");
	    subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ROOM__USER_ID", "==", userIds));
	    subQs.addExplicitFilter(
	        SimpleQueryFilter.makeColToValFilter("ROOM__IS_ACTIVE", "==", true, PixelDataType.BOOLEAN));
	    subQs.addExplicitFilter(
	        SimpleQueryFilter.makeColToValFilter("MESSAGE__MESSAGE_DATA", "!=", null));
	    subQs.addExplicitFilter(
	    	SimpleQueryFilter.makeColToValFilter("ROOM__WORKSPACE_ID", "==", workspaceId));
	    qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ROOM__ROOM_ID", "IN", subQs));
		
		SelectQueryStruct outerQs = new SelectQueryStruct();
		outerQs.addSelector(new QueryTypedColumnSelector("subquery__room_id", "room_id", SemossDataType.STRING));
		outerQs.addSelector(new QueryTypedColumnSelector("subquery__room_name", "room_name", SemossDataType.STRING));
		outerQs.addSelector(new QueryTypedColumnSelector("subquery__room_context", "room_context", SemossDataType.STRING));
		outerQs.addSelector(new QueryTypedColumnSelector("subquery__model_id", "model_id", SemossDataType.STRING));
		outerQs.addSelector(new QueryTypedColumnSelector("subquery__workspace_id", "workspace_id", SemossDataType.STRING));
		outerQs.addSelector(new QueryTypedColumnSelector("subquery__date_created", "date_created", SemossDataType.STRING));
		outerQs.addSelector(new QueryTypedColumnSelector("subquery__date_updated", "date_updated", SemossDataType.STRING));
		outerQs.addSelector(new QueryOpaqueSelector("COUNT(*) OVER()", "total_row_count"));
		
		if(filters != null && !filters.isEmpty()) {
			outerQs.mergeExplicitFilters(filters);
		}
		
		outerQs.setLimit(limit);
		outerQs.setOffSet(offset);
		
		if(sorts == null || sorts.isEmpty()) {
			outerQs.addOrderBy("date_created", "DESC");
		} else {
			outerQs.addOrderBy(sorts);
		}
		
		IQueryInterpreter interpreter = modelInferenceLogsDb.getQueryInterpreter();
		interpreter.setQueryStruct(qs);
		String subQuery = interpreter.composeQuery();
		outerQs.setCustomFrom(subQuery);
		outerQs.setCustomFromAliasName("subquery");
		
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, outerQs)) {
			Map<String, Object> workspaces = new HashMap<>();
			List<Map<String, Object>> roomDetails = new ArrayList<>();
			Long totalCount = 0L;
			while(wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				Map<String, Object> map = new HashMap<String, Object>();
				for(int i = 0; i < headers.length; i++) {
					if(values[i] instanceof java.sql.Clob) {
						String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
						map.put(headers[i], value);
					} else if(values[i] instanceof java.sql.Blob) {
						String value = AbstractSqlQueryUtil.flushBlobToString((java.sql.Blob) values[i]);
						map.put(headers[i], value);
					} else {
						map.put(headers[i], values[i]);
					}
				}
				Object totalCountObj = map.remove("total_row_count");
				if(totalCount == 0) {
					totalCount = (Long) totalCountObj;
				}
				roomDetails.add(map);
			}
			workspaces.put("total_count", totalCount);
			workspaces.put("rooms", roomDetails);
			return workspaces;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return null;
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param limit
	 * @param offset
	 * @param filters
	 * @param sorts
	 * @param sharedWorkspaceIds
	 */
	public static Map<String, Object> getWorkspaceEntriesForUser(User user, long limit, long offset, GenRowFilters filters, List<IQuerySort> sorts, Set<String> sharedWorkspaceIds) {
		Collection<String> userIds = getUserFiltersQs(user);
		
		SelectQueryStruct subQs = new SelectQueryStruct();
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__WORKSPACE_ID", "workspace_id"));
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__NAME", "name"));
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__DESCRIPTION", "description"));
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__SYSTEM_PROMPT", "system_prompt"));
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__OWNER", "owner"));
		subQs.addSelector(new QueryColumnSelector("WORKSPACE__SHARING_ENABLED", "sharing_enabled"));
		
		QueryFunctionSelector createdSelector = QueryFunctionSelector.makeFunctionSelector("TO_CHAR", "WORKSPACE__DATE_CREATED", "date_created");
		createdSelector.addAdditionalParam(new String[] {"'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'"});
		subQs.addSelector(createdSelector);
		
		QueryFunctionSelector updatedSelector = QueryFunctionSelector.makeFunctionSelector("TO_CHAR", "WORKSPACE__DATE_UPDATED", "date_updated");
		updatedSelector.addAdditionalParam(new String[] {"'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'"});
		subQs.addSelector(updatedSelector);
		
		subQs.addSelector(QueryIfSelector.makeQueryIfSelector(
				SimpleQueryFilter.makeColToValFilter("WORKSPACE__OWNER", "==", userIds),
				new QueryConstantSelector(Boolean.TRUE), 
				new QueryConstantSelector(Boolean.FALSE), 
				"is_creator")
		);
		
		OrQueryFilter userPermissionFilter = new OrQueryFilter(SimpleQueryFilter.makeColToValFilter("WORKSPACE__OWNER", "==", userIds));
		if(sharedWorkspaceIds != null && !sharedWorkspaceIds.isEmpty()) {
			userPermissionFilter.addFilter(SimpleQueryFilter.makeColToValFilter("WORKSPACE__WORKSPACE_ID", "==", sharedWorkspaceIds));
		}
		subQs.addExplicitFilter(userPermissionFilter);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryTypedColumnSelector("subquery__workspace_id", "workspace_id", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__name", "name", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__description", "description", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__system_prompt", "system_prompt", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__owner", "owner", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__sharing_enabled", "sharing_enabled", SemossDataType.BOOLEAN));
		qs.addSelector(new QueryTypedColumnSelector("subquery__date_created", "date_created", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__date_updated", "date_updated", SemossDataType.STRING));
		qs.addSelector(new QueryTypedColumnSelector("subquery__is_creator", "is_creator", SemossDataType.BOOLEAN));
		qs.addSelector(new QueryOpaqueSelector("COUNT(*) OVER()", "total_row_count"));
		
		if(filters != null && !filters.isEmpty()) {
			qs.mergeExplicitFilters(filters);
		}
		
		qs.setLimit(limit);
		qs.setOffSet(offset);
		
		if(sorts == null || sorts.isEmpty()) {
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
			while(wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				Map<String, Object> map = new HashMap<String, Object>();
				for(int i = 0; i < headers.length; i++) {
					if(values[i] instanceof java.sql.Clob) {
						String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
						map.put(headers[i], value);
					} else if(values[i] instanceof java.sql.Blob) {
						String value = AbstractSqlQueryUtil.flushBlobToString((java.sql.Blob) values[i]);
						map.put(headers[i], value);
					} else {
						map.put(headers[i], values[i]);
					}
				}
				Object totalCountObj = map.remove("total_row_count");
				if(totalCount == 0) {
					totalCount = (Long) totalCountObj;
				}
				workspaceDetails.add(map);
			}
			workspaces.put("total_count", totalCount);
			workspaces.put("workspaces", workspaceDetails);
			return workspaces;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return null;
		}
	}
	
	private static Collection<String> getUserFiltersQs(User user) {
		List<String> filters = new ArrayList<String>();
		if(user != null) {
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider thisLogin : logins) {
				filters.add(Utility.inputSQLSanitizer(user.getAccessToken(thisLogin).getId()));
			}
		}

		return filters;
	}
	
	public static List<Map<String, Object>> getWorkspaceKnowledge(String workspaceId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("WORKSPACE_KNOWLEDGE__WORKSPACE_KNOWLEDGE_ID", "workspace_knowledge_id"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE_KNOWLEDGE__WORKSPACE_ID", "workspace_id"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE_KNOWLEDGE__KNOWLEDGE_ID", "knowledge_id"));
		qs.addSelector(new QueryColumnSelector("WORKSPACE_KNOWLEDGE__KNOWLEDGE_TYPE", "knowledge_type"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("WORKSPACE_KNOWLEDGE__WORKSPACE_ID", "==", workspaceId));
		
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(modelInferenceLogsDb, qs)) {
			List<Map<String, Object>> results = new ArrayList<>();
			while(wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				Map<String, Object> map = new HashMap<String, Object>();
				for(int i = 0; i < headers.length; i++) {
					if(values[i] instanceof java.sql.Clob) {
						String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
						map.put(headers[i], value);
					} else if(values[i] instanceof java.sql.Blob) {
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
			classLogger.error(Constants.STACKTRACE, e);
			return null;
		}
	}
	
	public static void createNewWorkspaceKnowledge(String workspaceKnowledgeId, String workspaceId, String knowledgeId, String knowledgeType) throws Exception {
		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try(
				PreparedStatement ps = con.prepareStatement("INSERT INTO WORKSPACE_KNOWLEDGE (WORKSPACE_KNOWLEDGE_ID, WORKSPACE_ID, KNOWLEDGE_ID, KNOWLEDGE_TYPE) VALUES (?,?,?,?)")
			) {
				int index = 1;
				ps.setString(index++, workspaceKnowledgeId);
				ps.setString(index++, workspaceId);
				ps.setString(index++, knowledgeId);
				ps.setString(index++, knowledgeType);
				ps.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error creating workspace knowledge: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}

	/**
	 * 
	 * @param workspaceKnowledgeId
	 */
	public static void deleteWorkspaceKnowledge(String workspaceKnowledgeId) {
		Connection con = null;
		try {
			con = modelInferenceLogsDb.getConnection();
			try(
				PreparedStatement ps = con.prepareStatement("DELETE FROM WORKSPACE_KNOWLEDGE WHERE WORKSPACE_KNOWLEDGE_ID = ?");
			) {
				ps.setString(1, workspaceKnowledgeId);
				ps.execute();
				if (!con.getAutoCommit()) {
					con.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error deleting workspace knowledge: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, con, null, null);
		}
	}
	
	public static void enableWorkspaceProject(User user, String projectId) {
		if(!SecurityProjectUtils.userIsOwner(user, projectId)) {
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityProjectUtils.addProjectOwner(user, projectId, user.getAccessToken(ap).getId());
			}
		}
		
		boolean needsMetaUpdate = true;
		Map<String, Object> currentTags = SecurityProjectUtils.getAggregateProjectMetadata(projectId, Arrays.asList("tag"), true);
		Object oldTagObject = currentTags.get("tag");
		Object newTagObject = oldTagObject;
		if(oldTagObject != null) {
			if(oldTagObject instanceof List) {
				List<String> tags = (List<String>) oldTagObject;
				if(!tags.contains(ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG)) {
					tags.add(ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG);
				} else {
					needsMetaUpdate = false;
				}
			} else {
				String tag = (String) oldTagObject;
				if(!ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG.equals(tag)) {
					newTagObject = Arrays.asList(tag, ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG);
				} else {
					needsMetaUpdate = false;
				}
			}
		} else {
			newTagObject = Arrays.asList(ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG);
		}
		if(needsMetaUpdate) {
			Map<String, Object> metadata = new HashMap<>();
			metadata.put("tag", newTagObject);
			SecurityProjectUtils.updateProjectMetadata(projectId, metadata);
		}
	}
	
	public static void disableWorkspaceProject(String projectId) {
		try {
			SecurityProjectUtils.copyProjectPermissions(null, projectId);
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		boolean needsMetaUpdate = true;
		Map<String, Object> currentTags = SecurityProjectUtils.getAggregateProjectMetadata(projectId, Arrays.asList("tag"), true);
		Object oldTagObject = currentTags.get("tag");
		Object newTagObject = oldTagObject;
		if(oldTagObject != null) {
			if(oldTagObject instanceof List) {
				List<String> tags = (List<String>) oldTagObject;
				needsMetaUpdate = tags.remove(ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG);
			} else {
				String tag = (String) oldTagObject;
				if(ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG.equals(tag)) {
					newTagObject = new ArrayList<>();
				} else {
					needsMetaUpdate = false;
				}
			}
		} else {
			newTagObject = new ArrayList<>();
		}
		if(needsMetaUpdate) {
			Map<String, Object> metadata = new HashMap<>();
			metadata.put("tag", newTagObject);
			SecurityProjectUtils.updateProjectMetadata(projectId, metadata);
		}
	}
	
	public static IProject createWorkspaceProject(User user, String projectId, String projectName) {
		IProject project = null;
		File projectFolder = null;
		File projectTempSmss = null;
		File projectSmssFile = null;
		
		try {
			classLogger.info("Creating workspace project");
			
			projectFolder = SmssUtilities.validateProject(null, projectName, projectId);
			projectFolder.mkdirs();
			
			project = new Project();
			
			projectTempSmss = SmssUtilities.createTemporaryProjectSmss(projectId, projectName, IProject.PROJECT_TYPE.CODE, false, null, null, null, null);
			DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, projectTempSmss.getAbsolutePath());
			
			DIHelper.getInstance().setProjectProperty(projectId, project);
			String projects = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
			projects = projects + ";" + projectId;
			DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projects);
			
			projectSmssFile = new File(projectTempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(projectTempSmss, projectSmssFile);
			projectTempSmss.delete();
			project.open(projectSmssFile.getAbsolutePath());
			
			DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, projectSmssFile.getAbsolutePath());
			
			if (ClusterUtil.IS_CLUSTER) {
				classLogger.info("Syncing workspace project for cloud backup");
				ClusterUtil.pushProject(projectId);
			}
			
			SecurityProjectUtils.addProject(projectId, false, user);
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityProjectUtils.addProjectOwner(user, projectId, user.getAccessToken(ap).getId());
			}
			Map<String, Object> metadata = new HashMap<>();
			metadata.put("tag", ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG);
			SecurityProjectUtils.updateProjectMetadata(projectId, metadata);
			
			classLogger.info("Finished creating workspace project");
			
			return project;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			
			for(File file : Arrays.asList(projectTempSmss, projectSmssFile, projectFolder)) {
				try {
					if(file != null && file.exists()) {
						FileUtils.forceDelete(file);
					}
				} catch(Exception e2) {
					classLogger.error("Failed to delete file", e2);
				}
			}
			deleteWorkspaceProject(projectId, project);
			
			throw new SemossPixelException(e);
		}
	}
	
	public static void deleteWorkspaceProject(String projectId, IProject project) {
		UploadUtilities.removeProjectFromDIHelper(projectId);
		SecurityProjectUtils.deleteProject(projectId);
		UserTrackingUtils.deleteProject(projectId);

		if(project != null) {
			try {
				project.delete();
			} catch(Exception e) {
				classLogger.error("Error deleting workspace project " + projectId);
			}
		}
		if (ClusterUtil.IS_CLUSTER) {
			Thread deleteThread = new Thread(new DeleteProjectRunner(projectId));
			deleteThread.start();
		}
	}
	
	public static IVectorDatabaseEngine createWorkspaceVectorDb(User user, String vectorDbId, String vectorDbName, Map<String, Object> vectorDbDetails, VectorDatabaseTypeEnum vectorDbType) throws IllegalAccessException, Exception {
		IVectorDatabaseEngine vectorDb = null;
		File vectorDbFolder = null;
		File vectorDbTempSmss = null;
		File vectorDbSmssFile = null;
		
		try {
			classLogger.info("Creating workspace db");
			
			UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.VECTOR, user, vectorDbName, vectorDbId);
			vectorDbFolder = UploadUtilities.generateSpecificEngineFolder(IEngine.CATALOG_TYPE.VECTOR, vectorDbId, vectorDbName);
			
			String vectorDbClass = vectorDbType.getVectorDatabaseClass();
			vectorDb = (IVectorDatabaseEngine) Class.forName(vectorDbClass).getDeclaredConstructor().newInstance();
			
			vectorDbTempSmss = UploadUtilities.createTemporaryVectorDatabaseSmss(vectorDbId, vectorDbName, vectorDbClass, vectorDbDetails);
			DIHelper.getInstance().setEngineProperty(vectorDbId + "_" + Constants.STORE, vectorDbTempSmss.getAbsolutePath());
			
			DIHelper.getInstance().setEngineProperty(vectorDbId, vectorDb);
			String engines = (String) DIHelper.getInstance().getEngineProperty(Constants.ENGINES);
			engines = engines + ";" + vectorDbId;
			DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engines);
			
			vectorDbSmssFile = new File(vectorDbTempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(vectorDbTempSmss, vectorDbSmssFile);
			vectorDbTempSmss.delete();
			
			vectorDb.open(vectorDbSmssFile.getAbsolutePath());
			
			DIHelper.getInstance().setEngineProperty(vectorDbId + "_" + Constants.STORE, vectorDbSmssFile.getAbsolutePath());
			
			if (ClusterUtil.IS_CLUSTER) {
				classLogger.info("Syncing workspace db for cloud backup");
				ClusterUtil.pushEngine(vectorDbId);
			}
			
			SecurityEngineUtils.addEngine(vectorDbId, false, user);
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityEngineUtils.addEngineOwner(vectorDbId, user.getAccessToken(ap).getId());
			}
			Map<String, Object> metadata = new HashMap<>();
			metadata.put("tag", ModelInferenceLogsUtils.WORKSPACE_DATABASE_TAG);
			SecurityEngineUtils.updateEngineMetadata(vectorDbId, metadata);
			
			classLogger.info("Finished creating workspace db");
			
			return vectorDb;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			
			for(File file : Arrays.asList(vectorDbTempSmss, vectorDbSmssFile, vectorDbFolder)) {
				try {
					if(file != null && file.exists()) {
						FileUtils.forceDelete(file);
					}
				} catch(Exception e2) {
					classLogger.error("Failed to delete file", e2);
				}
			}
			deleteWorkspaceVectorDb(vectorDbId, vectorDb);
			
			throw new SemossPixelException(e);
		}
	}
	
	
	
	public static void deleteWorkspaceVectorDb(String dbId, IEngine db) {
		UploadUtilities.removeEngineFromDIHelper(dbId);
		SecurityEngineUtils.deleteEngine(dbId);
		UserTrackingUtils.deleteEngine(dbId);
		
		if(db != null) {
			try {
				db.delete();;
			} catch(Exception e) {
				classLogger.error("Error deleting db during workspace exception clean-up", e);
			}
		}
		
		EngineSyncUtility.clearEngineCache(dbId);
		if (ClusterUtil.IS_CLUSTER) {
			Thread deleteAppThread = new Thread(new DeleteEngineRunner(dbId, IEngine.CATALOG_TYPE.VECTOR));
			deleteAppThread.start();
		}
	}
	
	public static boolean isWorkspaceSharedWithUser(String workspaceId, User user, Integer... validPermissions) {
		return getWorkspaceSharePermission(workspaceId, user, validPermissions) < Integer.MAX_VALUE;
	}
	
	public static int getWorkspaceSharePermission(String workspaceId, User user, Integer... validPermissions) {
		List<String> projectIdFilter = Arrays.asList(workspaceId);
		
		Map<String, Object> projectMetadataFilter = new HashMap<>();
		projectMetadataFilter.put("tag", ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG);
		
		List<Integer> permissionFilter = null;
		if(validPermissions != null && validPermissions.length > 0) {
			permissionFilter = Arrays.asList(validPermissions);
			
		}
		
		List<Map<String, Object>> projectInfo = SecurityProjectUtils.getUserProjectList(user, null, projectIdFilter, 
				false, false, projectMetadataFilter, permissionFilter, null, null, null);
		
		int bestPermission = Integer.MAX_VALUE;
		for(Map<String, Object> info : projectInfo) {
			Integer permission = (Integer) info.get("permission");
			if(permission < bestPermission) {
				bestPermission = permission;
			}
		}
		return bestPermission;
	}

}
