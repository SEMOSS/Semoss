package prerna.util.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.MicrosoftSqlServerInterpreter;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.util.Constants;
import prerna.util.Utility;

public class MicrosoftSqlServerUtil extends AnsiSqlQueryUtil {
	
	MicrosoftSqlServerUtil() {
		super();
		setDbType(RdbmsTypeEnum.SQL_SERVER);
	}
	
	MicrosoftSqlServerUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.SQL_SERVER);
	}
	
	@Override
	public void initTypeConverstionMap() {
		super.initTypeConverstionMap();
		typeConversionMap.put("TIMESTAMP", "DATETIME");
		typeConversionMap.put("BOOLEAN", "BIT");
	}

	@Override
	public IQueryInterpreter getInterpreter(IEngine engine) {
		return new MicrosoftSqlServerInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new MicrosoftSqlServerInterpreter(frame);
	}
	
	/**
	 * Method to get the table and views for this connection
	 * @param con
	 * @param connectionUrl
	 * @param logger
	 * @return
	 */
	@Override
	public Map<String, List<String>> getTablesAndViews(Connection con, Statement tableStmt, ResultSet tablesRs, 
			Map<String, Object> connectionDetails, Logger logger) {
		String schemaFilter = (String) connectionDetails.get(AbstractSqlQueryUtil.SCHEMA);
		
		// keep a list of tables and views
		List<String> tableSchemas = new ArrayList<String>();
		List<String> tables = new ArrayList<String>();
		List<String> viewSchemas = new ArrayList<String>();
		List<String> views = new ArrayList<String>();

		String[] tableKeys = RdbmsConnectionHelper.getTableKeys(this.dbType);
		final String TABLE_NAME_STR = tableKeys[0];
		final String TABLE_TYPE_STR = tableKeys[1];
		final String TABLE_SCHEMA_STR = tableKeys[2];
		try {
			while (tablesRs.next()) {
				String table = tablesRs.getString(TABLE_NAME_STR);
				// this will be table or view
				String tableType = tablesRs.getString(TABLE_TYPE_STR).toUpperCase();
				// get schema
				String tableSchema = tablesRs.getString(TABLE_SCHEMA_STR).toUpperCase();
				if(schemaFilter != null && !schemaFilter.isEmpty()) {
					if(!schemaFilter.equalsIgnoreCase(tableSchema)) {
						continue;
					}
				}
				if(tableType.toUpperCase().contains("TABLE")) {
					logger.info("Found table = " + Utility.cleanLogString(table));
					tables.add(table);
					tableSchemas.add(tableSchema);
				} else {
					// there may be views built from sys or information schema
					// we want to ignore these
					logger.info("Found view = " + Utility.cleanLogString(table));
					views.add(table);
					viewSchemas.add(tableSchema);
				}
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeAutoClosable(tablesRs, logger);
			closeAutoClosable(tableStmt, logger);
			closeAutoClosable(con, logger);
		}
		logger.info("Done parsing database metadata");
		
		Map<String, List<String>> ret = new HashMap<String, List<String>>();
		ret.put("tables", tables);
		ret.put("tableSchemas", tableSchemas);
		ret.put("views", views);
		ret.put("viewSchemas", viewSchemas);
		return ret;
	}
	
	
	@Override
	public String buildConnectionString(Map<String, Object> configMap) throws RuntimeException {
		if(configMap.isEmpty()){
			throw new RuntimeException("Configuration map is empty");
		}
		
		String connectionString = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_STRING);
		if(connectionString != null && !connectionString.isEmpty()) {
			return connectionString;
		}
		
		String urlPrefix = this.dbType.getUrlPrefix();
		String hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if(hostname == null || hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		String port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		String database = (String) configMap.get(AbstractSqlQueryUtil.DATABASE);
		if(database == null || database.isEmpty()) {
			throw new RuntimeException("Must pass in database name");
		}
		
		connectionString = urlPrefix+"://"+hostname+port+";databaseName="+database;
		
		String additonalProperties = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);
		if(additonalProperties != null && !additonalProperties.isEmpty()) {
			if(!additonalProperties.startsWith(";") && !additonalProperties.startsWith("&")) {
				connectionString += ";" + additonalProperties;
			} else {
				connectionString += additonalProperties;
			}
		}
		
		return connectionString;
	}

	@Override
	public String buildConnectionString(Properties prop) throws RuntimeException {
		if(prop == null){
			throw new RuntimeException("Properties ojbect is null");
		}
		
		String connectionString = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_STRING);
		if(connectionString != null && !connectionString.isEmpty()) {
			return connectionString;
		}
		
		String urlPrefix = this.dbType.getUrlPrefix();
		String hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if(hostname == null || hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		String port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		String database = (String) prop.get(AbstractSqlQueryUtil.DATABASE);
		if(database == null || database.isEmpty()) {
			throw new RuntimeException("Must pass in database name");
		}
		
		connectionString = urlPrefix+"://"+hostname+port+";databaseName="+database;
		
		String additonalProperties = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);
		if(additonalProperties != null && !additonalProperties.isEmpty()) {
			if(!additonalProperties.startsWith(";") && !additonalProperties.startsWith("&")) {
				connectionString += ";" + additonalProperties;
			} else {
				connectionString += additonalProperties;
			}
		}
		
		return connectionString;
	}

	@Override
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {
		if(offset > 0 && limit > 0) {
			query = query.append(" OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY");
		} else if(offset > 0) {
			query = query.append(" OFFSET " + offset + " ROWS ");	
		} else if(limit > 0) {
			query = query.append(" OFFSET 0 ROWS FETCH NEXT " + limit + " ROWS ONLY");
		}
		
		return query;
	}
	
	@Override
	public StringBuffer addLimitOffsetToQuery(StringBuffer query, long limit, long offset) {
		if(offset > 0 && limit > 0) {
			query = query.append(" OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY");
		} else if(offset > 0) {
			query = query.append(" OFFSET " + offset + " ROWS ");	
		} else if(limit > 0) {
			query = query.append(" OFFSET 0 ROWS FETCH NEXT " + limit + " ROWS ONLY");
		}
		
		return query;
	}
	
	@Override
	public String removeDuplicatesFromTable(String tableName, String fullColumnNameList){
		return "SELECT DISTINCT " + fullColumnNameList 
					+ " INTO " + tableName + "_TEMP " 
					+ " FROM " + tableName + " WHERE " + tableName 
					+ " IS NOT NULL AND LTRIM(RTRIM(" + tableName + ")) <> ''";
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	@Override
	public String getGroupConcatFunctionSyntax() {
		return "STRING_AGG";
	}
	
	@Override
	public String processGroupByFunction(String selectExpression, String separator, boolean distinct) {
//		if(distinct) {
//			return getSqlFunctionSyntax(QueryFunctionHelper.GROUP_CONCAT) + "(DISTINCT " + selectExpression + ", '" + separator + "')";
//		} else {
			return getSqlFunctionSyntax(QueryFunctionHelper.GROUP_CONCAT) + "(" + selectExpression + ", '" + separator + "')";
//		}
	}
		
	@Override
	public boolean allowBooleanDataType() {
		return false;
	}

	@Override
	public String getDateWithTimeDataType() {
		return "DATETIME";
	}
	
	@Override
	public String getCurrentDate() {
		return "GETDATE()";
	}
	
	@Override
	public boolean allowBlobDataType() {
		return false;
	}
	
	@Override
	public String getBlobDataTypeName() {
		return "VARBINARY(MAX)";
	}
	
	@Override
	public String getBooleanDataTypeName() {
		return "BIT";
	}
	
	@Override
	public boolean allowsIfExistsTableSyntax() {
		return false;
	}
	
	@Override
	public boolean allowIfExistsAddConstraint() {
		return false;
	}
	
	@Override
	public String tableExistsQuery(String tableName, String schema) {
		return "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='" + schema + "' AND TABLE_NAME='" + tableName +"'";
	}
	
	@Override
	public String getAllColumnDetails(String tableName, String schema) {
		return "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG='" + schema + "' AND TABLE_NAME='" + tableName +"'";
	}
	
	@Override
	public String columnDetailsQuery(String tableName, String columnName, String schema) {
		return "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG='" + schema + "' AND TABLE_NAME='" + tableName +"'" + "' AND COLUMN_NAME='" + columnName.toUpperCase() + "'";
	}
	
	@Override
	public String constraintExistsQuery(String constraintName) {
		return "SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_NAME ='" + constraintName + "'";
	}
	
	@Override
	public String alterTableName(String tableName, String newTableName) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(newTableName)) {
			newTableName = getEscapeKeyword(newTableName);
		}
		return "sp_rename '" + tableName + "', '" + newTableName + "';";
	}
	
	@Override
	public String alterTableAddColumn(String tableName, String newColumn, String newColType) {
		if(!allowAddColumn()) {
			throw new UnsupportedOperationException("Does not support add column syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(newColumn)) {
			newColumn = getEscapeKeyword(newColumn);
		}
		return "ALTER TABLE " + tableName + " ADD " + newColumn + " " + newColType + ";";
	}
	
	@Override
	public String alterTableAddColumnWithDefault(String tableName, String newColumn, String newColType, Object defualtValue) {
		if(!allowAddColumn()) {
			throw new UnsupportedOperationException("Does not support add column syntax");
		}
		
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(newColumn)) {
			newColumn = getEscapeKeyword(newColumn);
		}
		return "ALTER TABLE " + tableName + " ADD " + newColumn + " " + newColType + " DEFAULT '" + defualtValue + "';";
	}
	
	@Override
	public String modColumnNotNull(String tableName, String columnName, String dataType) {
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " " + dataType + " NOT NULL";
	}
	
	@Override
	public String modColumnName(String tableName, String curColName, String newColName) {
		return "sp_rename '" + tableName + "." + curColName + "', '" + newColName + "', 'COLUMN';";
	}
	
	@Override
	public String dropIndex(String indexName, String tableName) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		return "DROP INDEX " + tableName + "." + indexName + ";";
	}
	
	@Override
	public String copyTable(String newTableName, String oldTableName) {
		if(isSelectorKeyword(newTableName)) {
			newTableName = getEscapeKeyword(newTableName);
		}
		if(isSelectorKeyword(oldTableName)) {
			oldTableName = getEscapeKeyword(oldTableName);
		}
		return "SELECT * INTO " + newTableName + " FROM " + oldTableName;
	}
	
}
