package prerna.engine.impl.rdbms;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class AuditDatabase {
	
	private static final Logger classLogger = LogManager.getLogger(AuditDatabase.class);

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final int INSERT_SIZE = 10;

	private static final String AUDIT_TABLE = "AUDIT_TABLE";
	private static final String QUERY_TABLE = "QUERY_TABLE";

	private Connection conn;
	private AbstractSqlQueryUtil queryUtil;

	private IDatabaseEngine database;
	private String databaseId;
	private String databaseName;

	@Deprecated
	private Map<String, String[]> primaryKeyCache = new HashMap<>();

	public AuditDatabase() {

	}

	/**
	 * First method that needs to be run to generate the actual connection details
	 * 
	 * @param database
	 * @param databaseId
	 * @param databaseName
	 */
	public void init(IDatabaseEngine database, String databaseId, String databaseName) {
		this.database = database;
		this.databaseId = databaseId;
		this.databaseName = databaseName;

		String dbFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		dbFolder += DIR_SEPARATOR + Constants.DATABASE_FOLDER + DIR_SEPARATOR + SmssUtilities.getUniqueName(databaseName, databaseId);

		String rdbmsTypeStr = DIHelper.getInstance().getProperty(Constants.DEFAULT_INSIGHTS_RDBMS);
		if (rdbmsTypeStr == null) {
			// default will be h2
			rdbmsTypeStr = "H2_DB";
		}
		RdbmsTypeEnum rdbmsType = RdbmsTypeEnum.valueOf(rdbmsTypeStr);

		String fileLocation = dbFolder + DIR_SEPARATOR + "audit_log_database";
		if (rdbmsType == RdbmsTypeEnum.H2_DB) {
			File f = new File(fileLocation + ".mv.db");
			if (!f.exists()) {
				try {
					f.createNewFile();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		} else {
			fileLocation += ".sqlite";
			File f = new File(fileLocation);
			if (!f.exists()) {
				try {
					f.createNewFile();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		String connectionUrl = null;
		if (rdbmsType == RdbmsTypeEnum.SQLITE) {
			connectionUrl = "jdbc:sqlite:" + fileLocation;
		} else {
			connectionUrl = "jdbc:h2:nio:" + fileLocation;
		}
		// regardless of OS, connection url is always /
		connectionUrl = connectionUrl.replace('\\', '/');

		classLogger.info("Audit connection url is " + connectionUrl);
		classLogger.info("Audit connection url is " + connectionUrl);
		classLogger.info("Audit connection url is " + connectionUrl);
		
//		RdbmsConnectionBuilder builder = new RdbmsConnectionBuilder(RdbmsConnectionBuilder.CONN_TYPE.DIRECT_CONN_URL);
//		builder.setConnectionUrl(connectionUrl);
//		builder.setDriver(rdbmsType.getDriver());
//		builder.setUserName("sa");
//		builder.setPassword("");
//		logger.info("Audit connection url is " + builder.getConnectionUrl());
//		logger.info("Audit connection url is " + builder.getConnectionUrl());
//		logger.info("Audit connection url is " + builder.getConnectionUrl());

		try {
			this.conn = AbstractSqlQueryUtil.makeConnection(rdbmsType, connectionUrl, "sa", "");
			this.queryUtil = SqlQueryUtilFactory.initialize(rdbmsType, connectionUrl, "sa", "");
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		// create the tables if necessary
		String[] headers = new String[] { "AUTO_INCREMENT", "ID", "TYPE", "TABLE", "KEY_COLUMN", "KEY_COLUMN_VALUE",
				"ALTERED_COLUMN", "OLD_VALUE", "NEW_VALUE", "TIMESTAMP", "USER" };
		String[] types = new String[] { "IDENTITY", "VARCHAR(50)", "VARCHAR(50)", "VARCHAR(200)", "VARCHAR(200)",
				"VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "TIMESTAMP", "VARCHAR(200)" };

		String auditTableQ = this.queryUtil.createTableIfNotExists(AUDIT_TABLE, headers, types);

		headers = new String[] { "ID", "TYPE", "QUERY" };
		types = new String[] { "VARCHAR(50)", "VARCHAR(50)", "CLOB" };
		String queryTableQ = this.queryUtil.createTableIfNotExists(QUERY_TABLE, headers, types);

		try(PreparedStatement auditTableStatement = conn.prepareStatement(auditTableQ);
			PreparedStatement queryTableStatement = conn.prepareStatement(queryTableQ);
		) {
			auditTableStatement.execute();
			queryTableStatement.execute();
		} catch(SQLException e){
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	/**
	 * 
	 * @param selectors
	 * @param values
	 * @param userId
	 */
	public synchronized void auditInsertQuery(List<IQuerySelector> selectors, List<Object> values, String userId,
			String query) {
		String primaryKeyTable = null;
		String primaryKeyColumn = null;
		String primaryKeyValue = null;

		for (int i = 0; i < selectors.size(); i++) {
			QueryColumnSelector s = (QueryColumnSelector) selectors.get(i);
			if (s.getColumn().equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
				String[] split = getPrimKey(s.getQueryStructName());
				primaryKeyTable = split[0];
				primaryKeyColumn = split[1];
				primaryKeyValue = values.get(i) + "";
			}
		}

		// define table where change is occurring
		if (primaryKeyTable == null) {
			QueryColumnSelector s = (QueryColumnSelector) selectors.get(0);
			primaryKeyTable = s.getTable();
		}

		StringBuilder auditInserts = new StringBuilder();

		String id = UUID.randomUUID().toString();
		java.sql.Timestamp time = Utility.getCurrentSqlTimestampUTC();

		Object[] insert = new Object[INSERT_SIZE];
		insert[0] = id;
		insert[1] = "INSERT";
		insert[2] = primaryKeyTable;
		insert[3] = primaryKeyColumn;
		insert[4] = primaryKeyValue;

		for(int i = 0; i < selectors.size(); i++) {
			QueryColumnSelector s = (QueryColumnSelector) selectors.get(i);
			String alteredColumn = s.getColumn();
			String newValue = values.get(i) + "";

			insert[5] = alteredColumn;
			insert[6] = null;
			insert[7] = newValue;
			insert[8] = time;
			insert[9] = userId;

			// get a combination of all the inserts
			auditInserts.append(getAuditInsert(insert));
			auditInserts.append(";");
		}

		String insertQ = auditInserts.toString();
		String auditQ = getAuditQueryLog(new Object[] { id, "INSERT", query });
		try (PreparedStatement insertStatement = conn.prepareStatement(insertQ);
			PreparedStatement auditStatement = conn.prepareStatement(auditQ);
		) {
			insertStatement.execute();
			auditStatement.execute();
		} catch(SQLException e){
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	/**
	 * 
	 * @param updateQs
	 * @param userId
	 */
	public synchronized void auditUpdateQuery(UpdateQueryStruct updateQs, String userId, String query) {
		List<IQuerySelector> selectors = updateQs.getSelectors();
		int numUpdates = selectors.size();
		List<Object> values = updateQs.getValues();

		// let us collect all the constraints
		// if this is just a primary key constraint
		// it will just be key_qs_name to key_column_value
		Map<String, String> constraintMap = getConstraintMap(updateQs);

		// loop through and find the key column
		String primaryKeyTable = null;
		String primaryKeyColumn = null;
		String primaryKeyValue = null;

		for (String filterQsName : constraintMap.keySet()) {
			if (!filterQsName.contains("__")) {
				// i guess you are the primary key
				String[] split = getPrimKey(filterQsName);
				primaryKeyTable = split[0];
				primaryKeyColumn = split[1];
				primaryKeyValue = constraintMap.get(filterQsName) + "";
			}
		}

		// define table where change is occurring
		if (primaryKeyTable == null) {
			QueryColumnSelector s = (QueryColumnSelector) selectors.get(0);
			primaryKeyTable = s.getTable();
		}

		StringBuilder auditUpdates = new StringBuilder();

		String id = UUID.randomUUID().toString();
		java.sql.Timestamp time = Utility.getCurrentSqlTimestampUTC();

		for (int i = 0; i < numUpdates; i++) {
			Object[] insert = new Object[INSERT_SIZE];
			insert[0] = id;
			insert[1] = "UPDATE";
			insert[2] = primaryKeyTable;
			insert[3] = primaryKeyColumn;
			insert[4] = primaryKeyValue;

			IQuerySelector selector = selectors.get(i);
			String alteredColumn = ((QueryColumnSelector) selector).getColumn();
			// are we updating the primary key ?
			if (alteredColumn.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
				alteredColumn = primaryKeyColumn;
			}

			String newValue = values.get(i) + "";
			String qsname = selector.getQueryStructName();
			String oldValue = constraintMap.get(qsname);

			insert[5] = alteredColumn;
			insert[6] = oldValue;
			insert[7] = newValue;
			insert[8] = time;
			insert[9] = userId;

			// get a combination of all the insert
			auditUpdates.append(getAuditInsert(insert));
			auditUpdates.append(";");
		}

		String insertQ = auditUpdates.toString();
		String updateQ = getAuditQueryLog(new Object[] { id, "UPDATE", query });
		try(
				PreparedStatement insertStatement = conn.prepareStatement(insertQ);
				PreparedStatement updateStatement = conn.prepareStatement(updateQ);
		){
			insertStatement.execute();
			updateStatement.execute();
			if(!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch(SQLException e){
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	/**
	 * 
	 * @param qs
	 * @param userId
	 */
	public synchronized void auditDeleteQuery(SelectQueryStruct qs, String userId, String query) {
		// when you delete
		// the qs should only have a single selector
		// which is the table name

		String primaryKeyTable = null;
		String primaryKeyColumn = null;
		String primaryKeyValue = null;

		List<IQuerySelector> selectors = qs.getSelectors();
		QueryColumnSelector s = (QueryColumnSelector) selectors.get(0);
		primaryKeyTable = s.getTable();
		primaryKeyColumn = s.getColumn();
		if (primaryKeyColumn.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
			String[] split = getPrimKey(primaryKeyTable);
			primaryKeyColumn = split[1];
		}

		Map<String, String> constraintMap = getConstraintMap(qs);
		if (constraintMap.containsKey(s.getQueryStructName())) {
			primaryKeyValue = constraintMap.get(s.getQueryStructName());
		}

		StringBuilder auditDeletes = new StringBuilder();

		String id = UUID.randomUUID().toString();
		java.sql.Timestamp time = Utility.getCurrentSqlTimestampUTC();

		for (String alteredColumn : constraintMap.keySet()) {
			if (alteredColumn.contains("__")) {
				alteredColumn = alteredColumn.split("__")[1];
			}
			String oldValue = constraintMap.get(alteredColumn);

			Object[] insert = new Object[INSERT_SIZE];
			insert[0] = id;
			insert[1] = "DELETE";
			insert[2] = primaryKeyTable;
			insert[3] = primaryKeyColumn;
			insert[4] = primaryKeyValue;

			// we are deleting based on the primary key
			insert[5] = alteredColumn;
			insert[6] = oldValue;
			insert[7] = null;
			insert[8] = time;
			insert[9] = userId;

			// get a combination of all the insert
			auditDeletes.append(getAuditInsert(insert));
			auditDeletes.append(";");
		}
		String deleteQ = query;
		try(
				PreparedStatement statement = conn.prepareStatement(auditDeletes.toString());
		        PreparedStatement deleteStatement = conn.prepareStatement(deleteQ);
		){
			statement.execute();
			deleteStatement.execute();
		} catch(SQLException e){
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	/**
	 * Store custom query into query log
	 * 
	 * @param userId
	 * @param query
	 */
	public void storeQuery(String userId, String query) {
		String q = getAuditQueryLog(new Object[] { userId, "CUSTOM", query });
		try(PreparedStatement statement = conn.prepareStatement(q)){
			statement.execute();
		} catch(SQLException e){
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	private String getAuditInsert(Object[] data) {
		String[] headers = new String[] { "ID", "TYPE", "TABLE", "KEY_COLUMN", "KEY_COLUMN_VALUE", "ALTERED_COLUMN",
				"OLD_VALUE", "NEW_VALUE", "TIMESTAMP", "USER" };
		String[] types = new String[] { "VARCHAR(50)", "VARCHAR(50)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)",
				"VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "TIMESTAMP", "VARCHAR(200)" };
		return this.queryUtil.insertIntoTable(AUDIT_TABLE, headers, types, data);
	}

	private String getAuditQueryLog(Object[] data) {
		String[] headers = new String[] { "ID", "TYPE", "QUERY" };
		String[] types = new String[] { "VARCHAR(50)", "VARCHAR(50)", "CLOB" };
		return this.queryUtil.insertIntoTable(QUERY_TABLE, headers, types, data);
	}

	/**
	 * Collect all the simple constraints from the qs This will get all qsName to
	 * value
	 * 
	 * @param qs
	 */
	private Map<String, String> getConstraintMap(AbstractQueryStruct qs) {
		Map<String, String> constraintMap = new HashMap<>();

		GenRowFilters grf = qs.getCombinedFilters();
		List<SimpleQueryFilter> filters = grf.getAllSimpleQueryFilters();
		for (SimpleQueryFilter f : filters) {
			// grab the values from the filter
			IQuerySelector col = null;
			Object colVal = null;
			if (f.getSimpleFilterType() == FILTER_TYPE.COL_TO_VALUES) {
				col = (IQuerySelector) f.getLComparison().getValue();
				colVal = f.getRComparison().getValue();
			} else if (f.getSimpleFilterType() == FILTER_TYPE.VALUES_TO_COL) {
				col = (IQuerySelector) f.getRComparison().getValue();
				colVal = f.getLComparison().getValue();
			}

			String qsname = null;
			String val = null;

			if (colVal instanceof List) {
				if (((List) colVal).size() == 1) {
					val = ((List) colVal).get(0).toString();
				} else {
					val = colVal.toString();
				}
			} else {
				val = colVal + "";
			}

			if (col != null) {
				qsname = col.getQueryStructName();
				constraintMap.put(qsname, val);
			}
		}

		return constraintMap;
	}

	/**
	 * 
	 * @param q
	 */
	private void execQ(String q){
		try(PreparedStatement statement = this.conn.prepareStatement(q)){
			statement.execute();
		} catch(SQLException e){
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	@Deprecated
	private String[] getPrimKey(String pixelName) {
		if (primaryKeyCache.containsKey(pixelName)) {
			return primaryKeyCache.get(pixelName);
		}

		// we dont have it.. so query for it
		String physicalUri = database.getPhysicalUriFromPixelSelector(pixelName);
		String column = database.getLegacyPrimKey4Table(physicalUri);
		String[] split = new String[] { pixelName, column };
		// store the value
		primaryKeyCache.put(pixelName, split);
		return split;
	}

	public void close() {
		try {
			this.conn.close();
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	public Connection getConnection() {
		return this.conn;
	}

	public AbstractSqlQueryUtil getQueryUtil() {
		return queryUtil;
	}

}
