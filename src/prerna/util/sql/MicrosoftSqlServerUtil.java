package prerna.util.sql;

import java.util.Collection;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.MicrosoftSqlServerInterpreter;
import prerna.query.querystruct.selectors.QueryFunctionHelper;

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
		typeConversionMap.put("DOUBLE", "DECIMAL(20,4)");
	}

	@Override
	public IQueryInterpreter getInterpreter(IEngine engine) {
		return new MicrosoftSqlServerInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new MicrosoftSqlServerInterpreter(frame);
	}
	
	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if(configMap == null || configMap.isEmpty()){
			throw new RuntimeException("Configuration map is null or empty");
		}
		
		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(hostname == null || hostname.isEmpty())
			) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		this.port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		this.database = (String) configMap.get(AbstractSqlQueryUtil.DATABASE);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.database == null || this.database.isEmpty())
				){
			throw new RuntimeException("Must pass in database name");
		}
		
		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+port+";databaseName="+this.database;
			
			if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
				if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
					this.connectionUrl += ";" + this.additionalProps;
				} else {
					this.connectionUrl += this.additionalProps;
				}
			}
		}
		
		return this.connectionUrl;
	}

	@Override
	public String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException {
		if(prop == null || prop.isEmpty()){
			throw new RuntimeException("Properties object is null or empty");
		}
		
		this.connectionUrl = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(hostname == null || hostname.isEmpty())
			) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		this.port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		this.database = (String) prop.get(AbstractSqlQueryUtil.DATABASE);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.database == null || this.database.isEmpty())
				){
			throw new RuntimeException("Must pass in database name");
		}
		
		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+port+";databaseName="+this.database;
			
			if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
				if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
					this.connectionUrl += ";" + this.additionalProps;
				} else {
					this.connectionUrl += this.additionalProps;
				}
			}
		}
		
		return this.connectionUrl;
	}

	@Override
	public String buildConnectionString() {
		if(this.connectionUrl != null && !this.connectionUrl.isEmpty()) {
			return this.connectionUrl;
		}
		
		if(this.hostname == null || this.hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		if(this.database == null || this.database.isEmpty()) {
			throw new RuntimeException("Must pass in schema name");
		}
		
		String port = getPort();
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+port+";databaseName="+this.database;
		
		if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
			if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
				this.connectionUrl += ";" + this.additionalProps;
			} else {
				this.connectionUrl += this.additionalProps;
			}
		}
		
		return this.connectionUrl;
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
	public String getCurrentTimestamp() {
		return "CURRENT_TIMESTAMP";
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
	public String tableExistsQuery(String tableName, String database, String schema) {
		return "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='" + database + "' AND TABLE_SCHEMA='" + schema + "' AND TABLE_NAME='" + tableName +"'";
	}
	
	@Override
	public String tableConstraintExistsQuery(String constraintName, String tableName, String database, String schema) {
		return "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME = '" + constraintName 
				+ "' AND TABLE_NAME = '" + tableName + "' AND TABLE_CATALOG='" + database + "' AND TABLE_SCHEMA='" + schema + "'";
	}

	@Override
	public String referentialConstraintExistsQuery(String constraintName, String database, String schema) {
		return "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_NAME = '" + constraintName 
				+ "' AND CONSTRAINT_CATALOG='" + database + "' AND CONSTRAINT_SCHEMA='" + schema + "'";
	}
	
	@Override
	public String getAllColumnDetails(String tableName, String database, String schema) {
		return "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG='" + database + "' AND TABLE_SCHEMA='" + schema 
				+ "' AND TABLE_NAME='" + tableName +"'";
	}
	
	@Override
	public String columnDetailsQuery(String tableName, String columnName, String database, String schema) {
		return "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG='" + database + "' AND TABLE_SCHEMA='" + schema 
				+ "' AND TABLE_NAME='" + tableName +"'" + "' AND COLUMN_NAME='" + columnName.toUpperCase() + "'";
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
	public String alterTableAddColumns(String tableName, String[] newColumns, String[] newColTypes) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		
		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " ADD ");
		for (int i = 0; i < newColumns.length; i++) {
			if (i > 0) {
				alterString.append(", ");
			}
			
			String newColumn = newColumns[i];
			// should escape keywords
			if(isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}
			
			alterString.append(newColumn + "  " + newColTypes[i]);
		}
		alterString.append(";");
		return alterString.toString();
	}
	
	@Override
	public String alterTableAddColumns(String tableName, Map<String, String> newColToTypeMap) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		
		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " ADD ");
		int i = 0;
		for(String newColumn : newColToTypeMap.keySet()) {
			String newColType = newColToTypeMap.get(newColumn);
			if (i > 0) {
				alterString.append(", ");
			}
			
			// should escape keywords
			if(isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}
			
			alterString.append(newColumn + "  " + newColType);
			
			i++;
		}
		alterString.append(";");
		return alterString.toString();
	}

	@Override
	public String alterTableAddColumnsWithDefaults(String tableName, String[] newColumns, String[] newColTypes, Object[] defaultValues) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		
		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " ADD ");
		for (int i = 0; i < newColumns.length; i++) {
			if (i > 0) {
				alterString.append(", ");
			}
			
			String newColumn = newColumns[i];
			// should escape keywords
			if(isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}
			
			alterString.append(newColumn + "  " + newColTypes[i]);
			
			// add default values
			if(defaultValues[i] != null) {
				alterString.append(" DEFAULT ");
				if(defaultValues[i]  instanceof String) {
					alterString.append("'").append(defaultValues[i]).append("'");
				} else {
					alterString.append(defaultValues[i]);
				}
			}
		}
		alterString.append(";");
		return alterString.toString();
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
	public String alterTableDropColumns(String tableName, Collection<String> columnNames) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		
		StringBuilder alterString = new StringBuilder("ALTER TABLE " + tableName + " DROP COLUMN ");
		int i = 0;
		for(String newColumn : columnNames) {
			if (i > 0) {
				alterString.append(", ");
			}
			
			// should escape keywords
			if(isSelectorKeyword(newColumn)) {
				newColumn = getEscapeKeyword(newColumn);
			}
			
			alterString.append(newColumn);
			
			i++;
		}
		alterString.append(";");
		return alterString.toString();
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
