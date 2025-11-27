package prerna.engine.impl.rdbms;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class AuditDatabase {

	private static final Logger classLogger = LogManager.getLogger(AuditDatabase.class);

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private static final String AUDIT_TABLE = "AUDIT_TABLE";
	private static final String QUERY_TABLE = "QUERY_TABLE";

	private RDBMSNativeEngine auditDatabase;
	private IDatabaseEngine database;
	private String databaseId;
	private String databaseName;

	private List<Pair<String, List<Pair<String, String>>>> allSchemas = null;
	private List<Pair<String, String>> auditColumns = null;
	private List<Pair<String, String>> queryColumns = null;

	@Deprecated
	private Map<String, String[]> primaryKeyCache = new HashMap<>();

	/**
	 * First method that needs to be run to generate the actual connection details
	 * 
	 * @param database
	 * @param databaseId
	 * @param databaseName
	 * @throws Exception
	 */
	public void init(IDatabaseEngine database, String databaseId, String databaseName) throws Exception {
		this.database = database;
		this.databaseId = databaseId;
		this.databaseName = databaseName;

		String dbFolder = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.DATABASE, databaseId,
				databaseName);

		String rdbmsTypeStr = Utility.getDIHelperProperty(Constants.DEFAULT_INSIGHTS_RDBMS);
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
		if (rdbmsType == RdbmsTypeEnum.H2_DB) {
			connectionUrl = "jdbc:h2:nio:" + fileLocation;
		} else {
			connectionUrl = "jdbc:sqlite:" + fileLocation;
		}
		// regardless of OS, connection url is always /
		connectionUrl = connectionUrl.replace('\\', '/');

		classLogger.info("Audit connection url is {}", connectionUrl);

		Properties tempSmssProp = new Properties();
		tempSmssProp.put(Constants.CONNECTION_URL, connectionUrl);
		tempSmssProp.put(Constants.USERNAME, "sa");
		tempSmssProp.put(Constants.PASSWORD, "");
		tempSmssProp.put(Constants.DRIVER, rdbmsType.getDriver());
		tempSmssProp.put(Constants.RDBMS_TYPE, rdbmsType.getLabel());
		tempSmssProp.put("TEMP", "TRUE");
		tempSmssProp.put(Constants.ENGINE, this.databaseId + "_?Audit");
		this.auditDatabase = new RDBMSNativeEngine();
		auditDatabase.setBasic(true);
		auditDatabase.open(tempSmssProp);

		Connection conn = auditDatabase.getConnection();
		AbstractSqlQueryUtil queryUtil = auditDatabase.getQueryUtil();

		final String BLOB_DATATYPE_NAME = queryUtil.getBlobDataTypeName();
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
		final String DOUBLE_DATATYPE_NAME = queryUtil.getDoubleDataTypeName();

		this.auditColumns = Arrays.asList(Pair.with("AUTO_INCREMENT", "IDENTITY"), Pair.with("ID", "VARCHAR(50)"),
				Pair.with("TYPE", "VARCHAR(50)"), Pair.with("TABLE", "VARCHAR(200)"),
				Pair.with("KEY_COLUMN", "VARCHAR(200)"), Pair.with("KEY_COLUMN_VALUE", "VARCHAR(200)"),
				Pair.with("ALTERED_COLUMN", "VARCHAR(200)"), Pair.with("OLD_VALUE", "VARCHAR(200)"),
				Pair.with("NEW_VALUE", "VARCHAR(200)"), Pair.with("TIMESTAMP", TIMESTAMP_DATATYPE_NAME),
				Pair.with("USER", "VARCHAR(200)"));

		this.queryColumns = Arrays.asList(Pair.with("ID", "VARCHAR(50)"), Pair.with("USERID", "VARCHAR(50)"),
				Pair.with("TYPE", "VARCHAR(50)"), Pair.with("QUERY", "CLOB"));

		this.allSchemas = Arrays.asList(Pair.with(QUERY_TABLE, this.queryColumns),
				Pair.with(AUDIT_TABLE, this.auditColumns));

		for (Pair<String, List<Pair<String, String>>> tableSchema : allSchemas) {
			String tableName = tableSchema.getValue0();
			String[] colNames = tableSchema.getValue1().stream().map(Pair::getValue0).toArray(String[]::new);
			String[] types = tableSchema.getValue1().stream().map(Pair::getValue1).toArray(String[]::new);
			String sql = queryUtil.createTableIfNotExists(tableName, colNames, types);
			auditDatabase.insertData(sql);

			List<String> allCols = queryUtil.getTableColumns(conn, tableName, null, null);
			for (int i = 0; i < colNames.length; i++) {
				String col = colNames[i];
				if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
					String addColumnSql = queryUtil.alterTableAddColumn(tableName, col, types[i]);
					auditDatabase.insertData(addColumnSql);
				}
			}
		}
	}

	/**
	 * 
	 * @param selectors
	 * @param values
	 * @param userId
	 * @param query
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

		String id = GUID.v7().toUUID().toString();
		java.sql.Timestamp time = Utility.getCurrentSqlTimestampUTC();

		String auditInsertQuery = "INSERT INTO " + AUDIT_TABLE
				+ " (ID, TYPE, \"TABLE\", KEY_COLUMN, KEY_COLUMN_VALUE, ALTERED_COLUMN, OLD_VALUE, NEW_VALUE, \"TIMESTAMP\", \"USER\") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		try (PreparedStatement ps = auditDatabase.getConnection().prepareStatement(auditInsertQuery)) {
			for (int i = 0; i < selectors.size(); i++) {
				QueryColumnSelector s = (QueryColumnSelector) selectors.get(i);
				String alteredColumn = s.getColumn();
				String newValue = values.get(i) + "";

				int pIndex = 1;
				ps.setString(pIndex++, id);
				ps.setString(pIndex++, "INSERT");
				ps.setString(pIndex++, primaryKeyTable);
				ps.setString(pIndex++, primaryKeyColumn);
				ps.setString(pIndex++, primaryKeyValue);
				ps.setString(pIndex++, alteredColumn);
				ps.setNull(pIndex++, java.sql.Types.VARCHAR);
				ps.setString(pIndex++, newValue);
				ps.setTimestamp(pIndex++, time);
				ps.setString(pIndex++, userId);
				ps.addBatch();
			}

			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		storeExactQuery(id, userId, "INSERT", query);
	}

	/**
	 * 
	 * @param updateQs
	 * @param userId
	 * @param query
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

		String id = GUID.v7().toUUID().toString();
		java.sql.Timestamp time = Utility.getCurrentSqlTimestampUTC();

		String auditUpdateQuery = "INSERT INTO " + AUDIT_TABLE
				+ " (ID, TYPE, \"TABLE\", KEY_COLUMN, KEY_COLUMN_VALUE, ALTERED_COLUMN, OLD_VALUE, NEW_VALUE, \"TIMESTAMP\", \"USER\") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		try (PreparedStatement ps = auditDatabase.getConnection().prepareStatement(auditUpdateQuery)) {
			for (int i = 0; i < numUpdates; i++) {
				IQuerySelector selector = selectors.get(i);
				String alteredColumn = ((QueryColumnSelector) selector).getColumn();
				// are we updating the primary key ?
				if (alteredColumn.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
					alteredColumn = primaryKeyColumn;
				}

				String newValue = values.get(i) + "";
				String qsname = selector.getQueryStructName();
				String oldValue = constraintMap.get(qsname);

				int pIndex = 1;
				ps.setString(pIndex++, id);
				ps.setString(pIndex++, "UPDATE");
				ps.setString(pIndex++, primaryKeyTable);
				ps.setString(pIndex++, primaryKeyColumn);
				ps.setString(pIndex++, primaryKeyValue);
				ps.setString(pIndex++, alteredColumn);
				ps.setString(pIndex++, oldValue);
				ps.setString(pIndex++, newValue);
				ps.setTimestamp(pIndex++, time);
				ps.setString(pIndex++, userId);
				ps.addBatch();
			}

			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		storeExactQuery(id, userId, "UPDATE", query);
	}

	/**
	 * 
	 * @param qs
	 * @param userId
	 * @param query
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

		String id = GUID.v7().toUUID().toString();
		java.sql.Timestamp time = Utility.getCurrentSqlTimestampUTC();

		String auditDeleteQuery = "INSERT INTO " + AUDIT_TABLE
				+ " (ID, TYPE, \"TABLE\", KEY_COLUMN, KEY_COLUMN_VALUE, ALTERED_COLUMN, OLD_VALUE, NEW_VALUE, \"TIMESTAMP\", \"USER\") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		try (PreparedStatement ps = auditDatabase.getConnection().prepareStatement(auditDeleteQuery)) {
			for (String alteredColumn : constraintMap.keySet()) {
				if (alteredColumn.contains("__")) {
					alteredColumn = alteredColumn.split("__")[1];
				}
				String oldValue = constraintMap.get(alteredColumn);

				int pIndex = 1;
				ps.setString(pIndex++, id);
				ps.setString(pIndex++, "DELETE");
				ps.setString(pIndex++, primaryKeyTable);
				ps.setString(pIndex++, primaryKeyColumn);
				ps.setString(pIndex++, primaryKeyValue);
				ps.setString(pIndex++, alteredColumn);
				ps.setString(pIndex++, oldValue);
				ps.setNull(pIndex++, java.sql.Types.VARCHAR);
				ps.setTimestamp(pIndex++, time);
				ps.setString(pIndex++, userId);
				ps.addBatch();
			}

			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		storeExactQuery(id, userId, "DELETE", query);
	}

	/**
	 * 
	 * @param userId
	 * @param queryType
	 * @param query
	 */
	public void storeExactQuery(String userId, String queryType, String query) {
		storeExactQuery(null, userId, queryType, query);
	}

	/**
	 * 
	 * @param id
	 * @param userId
	 * @param queryType
	 * @param query
	 */
	private void storeExactQuery(String id, String userId, String queryType, String query) {
		if (id == null) {
			id = GUID.v7().toUUID().toString();
		}
		String insertQuery = "INSERT INTO " + QUERY_TABLE + "(ID, USERID, TYPE, QUERY) VALUES (?,?,?,?)";
		try (PreparedStatement ps = auditDatabase.getConnection().prepareStatement(insertQuery)) {
			int pIdx = 1;
			ps.setString(pIdx++, id);
			ps.setString(pIdx++, userId);
			ps.setString(pIdx++, queryType);
			auditDatabase.getQueryUtil().handleInsertionOfClob(ps, query, pIdx++, GSON);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException | UnsupportedEncodingException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
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
			this.auditDatabase.close();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	public RDBMSNativeEngine getAuditDatabase() {
		return this.auditDatabase;
	}

}
