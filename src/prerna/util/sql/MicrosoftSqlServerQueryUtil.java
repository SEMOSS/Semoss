package prerna.util.sql;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.MicrosoftSqlServerInterpreter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MicrosoftSqlServerQueryUtil extends AnsiSqlQueryUtil {
	
	MicrosoftSqlServerQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.SQL_SERVER);
	}
	
	MicrosoftSqlServerQueryUtil(String connectionUrl, String username, String password) {
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
	public IQueryInterpreter getInterpreter(IDatabaseEngine engine) {
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
		this.port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		this.database = (String) configMap.get(AbstractSqlQueryUtil.DATABASE);
		this.schema = (String) configMap.get(AbstractSqlQueryUtil.SCHEMA);
		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);
		this.username = (String) configMap.get(AbstractSqlQueryUtil.USERNAME);
		this.password = (String) configMap.get(AbstractSqlQueryUtil.PASSWORD);

		return buildConnectionString();
	}

	@Override
	public String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException {
		if(prop == null || prop.isEmpty()){
			throw new RuntimeException("Properties object is null or empty");
		}
		
		this.connectionUrl = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_URL);
		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		this.port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		this.database = (String) prop.get(AbstractSqlQueryUtil.DATABASE);
		this.schema = (String) prop.get(AbstractSqlQueryUtil.SCHEMA);
		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);
		this.username = (String) prop.get(AbstractSqlQueryUtil.USERNAME);
		this.password = (String) prop.get(AbstractSqlQueryUtil.PASSWORD);
		
		return buildConnectionString();
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
			throw new RuntimeException("Must pass in database name");
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
	public StringBuilder getFirstRow(StringBuilder query) {
			String strquery = query.toString();
			strquery=strquery.replaceFirst("(?i)SELECT", "SELECT TOP 1");
			query = new StringBuilder();
			query.append(strquery);
			return query;
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
	public String getClobDataTypeName() {
		return "VARCHAR(MAX)";
	}
	
	@Override
	public String getBooleanDataTypeName() {
		return "BIT";
	}
	
	@Override
	public String getRegexLikeFunctionSyntax() {
		return "PATINDEX";
	}
	
	@Override
	public IQueryFilter getSearchRegexFilter(String columnQs, String searchTerm) {
		// WHERE PATINDEX ('%pattern%',expression) != 0
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction("PATINDEX");
		fun.addInnerSelector(new QueryConstantSelector("%"+searchTerm+"%"));
		fun.addInnerSelector(new QueryColumnSelector(columnQs));
		NounMetadata lComparison = new NounMetadata(fun, PixelDataType.COLUMN);
		NounMetadata rComparison = new NounMetadata(0, PixelDataType.CONST_INT);
		SimpleQueryFilter filter = new SimpleQueryFilter(lComparison, "!=", rComparison);
		return filter;
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
	public boolean allowIfExistsModifyColumnSyntax() {
		return false;
	}
	
	@Override
	public boolean allowIfExistsIndexSyntax() {
		return false;
	}
	
	@Override
	public boolean savePointAutoRelease() {
		// do not call release savepoint method - will throw error/exception 
		return true;
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
				+ "' AND TABLE_NAME='" + tableName +"' AND COLUMN_NAME='" + columnName.toUpperCase() + "'";
	}
	
	@Override
	public String getIndexDetails(String indexName, String tableName, String database, String schema) {
		return "SELECT ix.name as IndexName, tab.name as TableName, COL_NAME(ix.object_id, ixc.column_id) as ColumnName, "
				+ "ix.type_desc, ix.is_disabled FROM sys.indexes ix " 
				+ "INNER JOIN sys.index_columns ixc ON  ix.object_id = ixc.object_id and ix.index_id = ixc.index_id "
				+ "INNER JOIN sys.tables tab ON ix.object_id = tab.object_id "
				+ "WHERE "
				+ "ix.is_primary_key = 0 "            /* Remove Primary Keys */
				+ "AND ix.is_unique = 0 "             /* Remove Unique Keys */
				+ "AND ix.is_unique_constraint = 0 "  /* Remove Unique Constraints */
				+ "AND tab.is_ms_shipped = 0"         /* Remove SQL Server Default Tables */
				+ "AND ix.name='" + indexName + "' "
				+ "AND tab.name='" + tableName + "'"
				;
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
	
	@Override
	public String insertIntoTable(String tableName, String[] columnNames, String[] types, Object[] values) {
		if(columnNames.length !=  types.length) {
			throw new UnsupportedOperationException("Headers and types must have the same length");
		}
		if(columnNames.length != values.length) {
			throw new UnsupportedOperationException("Headers and values must have the same length");
		}

		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}

		// only loop 1 time around both arrays since length must always match
		StringBuilder inserter = new StringBuilder("INSERT INTO " + tableName + " (");
		StringBuilder template = new StringBuilder();

		for (int colIndex = 0; colIndex < columnNames.length; colIndex++) {
			String columnName = columnNames[colIndex];
			String type = types[colIndex];
			Object value = values[colIndex];

			if(colIndex > 0) {
				inserter.append(", ");
				template.append(", ");
			}

			if(isSelectorKeyword(columnName)) {
				columnName = getEscapeKeyword(columnName);
			}

			// always jsut append the column name
			inserter.append(columnName);

			if(value == null) {
				// append null without quotes
				template.append("null");
				continue;
			}

			// we do not have a null
			// now we care how we insert based on the type of the value
			SemossDataType dataType = SemossDataType.convertStringToDataType(type);
			if(dataType == SemossDataType.INT ||
					dataType == SemossDataType.DOUBLE) {
				// append as is
				template.append(value);
			} else if(dataType == SemossDataType.BOOLEAN || 
					dataType == SemossDataType.STRING || dataType == SemossDataType.FACTOR) {
				template.append("'").append(escapeForSQLStatement(value + "")).append("'");
			} else if(dataType == SemossDataType.DATE) {
				if(value instanceof SemossDate) {
					Date d = ((SemossDate) value).getDate();
					if(d == null) {
						template.append(null + "");
					} else {
						template.append("'").append(((SemossDate) value).getFormatted("yyyy-MM-dd")).append("'");
					}
				} else if(value instanceof java.sql.Date) {
					template.append("'").append(value.toString()).append("'");
				} else {
					SemossDate dateValue = SemossDate.genDateObj(value + "");
					if(dateValue == null) {
						template.append(null + "");
					} else {
						template.append("'").append(dateValue.getFormatted("yyyy-MM-dd")).append("'");
					}
				}
			} else if(dataType == SemossDataType.TIMESTAMP) {
				if(value instanceof SemossDate) {
					Date d = ((SemossDate) value).getDate();
					if(d == null) {
						template.append(null + "");
					} else {
						template.append("'").append(((SemossDate) value).getFormatted("yyyy-MM-dd HH:mm:ss")).append("'");
					}
				} else if(value instanceof java.sql.Timestamp) {
					template.append("'").append(value.toString()).append("'");
				} else {
					SemossDate dateValue = SemossDate.genTimeStampDateObj(value + "");
					if(dateValue == null) {
						template.append(null + "");
					} else {
						template.append("'").append(dateValue.getFormatted("yyyy-MM-dd HH:mm:ss")).append("'");
					}
				}
			}
		}

		inserter.append(")  VALUES (").append(template).append(")");
		return inserter.toString();
	}
	
	@Override
	public String getDatabaseMetadataCatalogFilter() {
		return this.database;
	}
	
	@Override
	public String getDatabaseMetadataSchemaFilter() {
		return this.schema;
	}
}
