package prerna.util.sql;

import java.util.Collection;
import java.util.Iterator;

import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class AnsiSqlQueryUtil extends AbstractRdbmsQueryUtil {

	AnsiSqlQueryUtil() {
		super();
	}
	
	public AnsiSqlQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
	}
	
	public AnsiSqlQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
	/*
	 * Query helper for interpreters
	 * Here is the abstract which is for typical ANSI SQL
	 * However, each query util can override and use its
	 * own specific function names
	 */
	
	@Override
	public String getSqlFunctionSyntax(String inputFunction) {
		String findFunction = inputFunction.toLowerCase();
		if(findFunction.equals(QueryFunctionHelper.MIN)) {
			return getMinFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.MAX)) {
			return getMaxFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.MEAN) 
				|| findFunction.equals(QueryFunctionHelper.AVERAGE_1) 
				|| findFunction.equals(QueryFunctionHelper.AVERAGE_2)) { 
			return getAvgFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.UNIQUE_MEAN) 
				|| findFunction.equals(QueryFunctionHelper.UNIQUE_AVERAGE_1) 
				|| findFunction.equals(QueryFunctionHelper.UNIQUE_AVERAGE_2)) {
			return getAvgFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.MEDIAN)) {
			return getMedianFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.SUM) 
				|| findFunction.equals(QueryFunctionHelper.UNIQUE_SUM)) {
			return getSumFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.STDEV_1) 
				|| findFunction.equals(QueryFunctionHelper.STDEV_2)) {
			return getStdevFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.COUNT)) {
			return getCountFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.UNIQUE_COUNT)) {
			return getCountFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.CONCAT)) {
			return getConcatFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.GROUP_CONCAT)) {
			return getGroupConcatFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.UNIQUE_GROUP_CONCAT)) {
			return getGroupConcatFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.LOWER)) {
			return getLowerFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.COALESCE)) {
			return getCoalesceFunctionSyntax();
		} else if(findFunction.equals(QueryFunctionHelper.REGEXP_LIKE)) {
			return getRegexLikeFunctionSyntax();
		}
		
		return inputFunction;
	}
	
	@Override
	public String getMinFunctionSyntax() {
		return "MIN";
	}
	
	@Override
	public String getMaxFunctionSyntax() {
		return "MAX";
	}
	
	@Override
	public String getAvgFunctionSyntax() {
		return "AVG";
	}
	
	@Override
	public String getMedianFunctionSyntax() {
		return "MEDIAN";
	}
	
	@Override
	public String getSumFunctionSyntax() {
		return "SUM";
	}
	
	@Override
	public String getStdevFunctionSyntax() {
		return "STDDEV_SAMP";
	}
	
	@Override
	public String getCountFunctionSyntax() {
		return "COUNT";
	}
	
	@Override
	public String getConcatFunctionSyntax() {
		return "CONCAT";
	}
	
	@Override
	public String getGroupConcatFunctionSyntax() {
		return "GROUP_CONCAT";
	}
	
	@Override
	public String getLowerFunctionSyntax() {
		return "LOWER";
	}
	
	@Override
	public String getCoalesceFunctionSyntax() {
		return "COALESCE";
	}
	
	@Override
	public String getRegexLikeFunctionSyntax() {
		return "REGEXP_LIKE";
	}
	
	@Override
	public void preProcessFunctionSelector(QueryFunctionSelector selector) {
		// nothing to do for base
		
	}

	/////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////

	/*
	 * This section is intended for modifications to select queries to pull data
	 */
	
	@Override
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {
		if(limit > 0) {
			query = query.append(" LIMIT "+limit);
		}
		if(offset > 0) {
			query = query.append(" OFFSET "+offset);
		}
		return query;
	}
	
	@Override
	public String removeDuplicatesFromTable(String tableName, String fullColumnNameList){
		return "CREATE TABLE " + tableName + "_TEMP AS "
					+ "(SELECT DISTINCT " + fullColumnNameList
					+ " FROM " + tableName + " WHERE " + tableName 
					+ " IS NOT NULL AND TRIM(" + tableName + ") <> '' )";
	}

	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Some booleans so we know if we can use optimized scripts
	 * Or if we have to query the engine directly
	 * 
	 * Set it up in the base such that we will check these values
	 * in the "if exists" methods so that the implementations that override these
	 * methods only need to override these booleans and not every subsequent method
	 */
	
	@Override
	public boolean allowAddColumn() {
		return true;
	}
	
	@Override
	public boolean allowRedefineColumn() {
		return true;
	}
	
	@Override
	public boolean allowDropColumn() {
		return true;
	}
	
	@Override
	public boolean allowsIfExistsTableSyntax() {
		return true;
	}

	@Override
	public boolean allowIfExistsIndexSyntax() {
		return true;
	}
	
	@Override
	public boolean allowIfExistsModifyColumnSyntax() {
		return true;
	}
	
	/////////////////////////////////////////////////////////////////////////
	
	/*
	 * Create table scripts
	 */
	
	@Override
	public String createTable(String tableName, String[] colNames, String[] types) {
		StringBuilder retString = new StringBuilder("CREATE TABLE "+ tableName + " (" + colNames[0] + " " + types[0]);
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			retString = retString.append(" , " + colNames[colIndex] + "  " + types[colIndex]);
		}
		retString = retString.append(");");
		return retString.toString();
	}
	
	@Override
	public String createTableWithDefaults(String tableName, String [] colNames, String [] types, Object[] defaultValues) {
		StringBuilder retString = new StringBuilder("CREATE TABLE "+ tableName + " (" + colNames[0] + " " + types[0]);
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			retString.append(" , " + colNames[colIndex] + "  " + types[colIndex]);
			// add default values
			if(defaultValues[colIndex] != null) {
				retString.append(" DEFAULT ");
				if(defaultValues[colIndex] instanceof String) {
					retString.append("'").append(defaultValues[colIndex]).append("'");
				} else {
					retString.append(defaultValues[colIndex]);
				}
			}
		}
		retString = retString.append(");");
		return retString.toString();
	}
	
	@Override
	public String createTableIfNotExists(String tableName, String[] colNames, String[] types) {
		if(!allowsIfExistsTableSyntax()) {
			throw new IllegalArgumentException("Does not support if exists table syntax");
		}
		StringBuilder retString = new StringBuilder("CREATE TABLE IF NOT EXISTS "+ tableName + " (" + colNames[0] + " " + types[0]);
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			retString = retString.append(" , " + colNames[colIndex] + "  " + types[colIndex]);
		}
		retString = retString.append(");");
		return retString.toString();
	}
	
	@Override
	public String createTableIfNotExistsWithDefaults(String tableName, String [] colNames, String [] types, Object[] defaultValues) {
		if(!allowsIfExistsTableSyntax()) {
			throw new IllegalArgumentException("Does not support if exists table syntax");
		}
		StringBuilder retString = new StringBuilder("CREATE TABLE IF NOT EXISTS "+ tableName + " (" + colNames[0] + " " + types[0]);
		for(int colIndex = 1; colIndex < colNames.length; colIndex++) {
			retString.append(" , " + colNames[colIndex] + "  " + types[colIndex]);
			// add default values
			if(defaultValues[colIndex] != null) {
				retString.append(" DEFAULT ");
				if(defaultValues[colIndex] instanceof String) {
					retString.append("'").append(defaultValues[colIndex]).append("'");
				} else {
					retString.append(defaultValues[colIndex]);
				}
			}
		}
		retString = retString.append(");");
		return retString.toString();
	}
	
	/*
	 * Drop table scripts
	 */

	@Override
	public String dropTable(String tableName) {
		return "DROP TABLE " + tableName + ";";
	}
	
	@Override
	public String dropTableIfExists(String tableName) {
		if(!allowsIfExistsTableSyntax()) {
			throw new IllegalArgumentException("Does not support if exists table syntax");
		}
		return "DROP TABLE IF EXISTS " + tableName + ";";
	}
	
	/*
	 * Alter table scripts
	 */
	
	@Override
	public String alterTableName(String tableName, String newTableName) {
		return "ALTER TABLE " + tableName + " RENAME TO " + newTableName;
	}

	@Override
	public String alterTableAddColumn(String tableName, String newColumn, String newColType) {
		if(!allowAddColumn()) {
			throw new IllegalArgumentException("Does not support add column syntax");
		}
		return "ALTER TABLE " + tableName + " ADD COLUMN " + newColumn + " " + newColType + ";";
	}

	@Override
	public String alterTableAddColumnWithDefault(String tableName, String newColumn, String newColType, Object defualtValue) {
		if(!allowAddColumn()) {
			throw new IllegalArgumentException("Does not support add column syntax");
		}
		return "ALTER TABLE " + tableName + " ADD COLUMN " + newColumn + " " + newColType + " DEFAULT '" + defualtValue + "';";
	}

	@Override
	public String alterTableAddColumnIfNotExists(String tableName, String newColumn, String newColType) {
		if(!allowAddColumn()) {
			throw new IllegalArgumentException("Does not support add column syntax");
		}
		if(!allowIfExistsModifyColumnSyntax()) {
			throw new IllegalArgumentException("Does not support if exists column syntax");
		}
		return "ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS " + newColumn + " " + newColType + ";";
	}

	@Override
	public String alterTableAddColumnIfNotExistsWithDefault(String tableName, String newColumn, String newColType, Object defualtValue) {
		if(!allowAddColumn()) {
			throw new IllegalArgumentException("Does not support add column syntax");
		}
		if(!allowIfExistsModifyColumnSyntax()) {
			throw new IllegalArgumentException("Does not support if exists column syntax");
		}
		return "ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS " + newColumn + " " + newColType + " DEFAULT '" + defualtValue + "';";
	}

	@Override
	public String alterTableDropColumn(String tableName, String columnName) {
		if(!allowDropColumn()) {
			throw new IllegalArgumentException("Does not support drop column syntax");
		}
		return "ALTER TABLE " + tableName + " DROP COLUMN " + columnName + ";";
	}

	@Override
	public String alterTableDropColumnIfExists(String tableName, String columnName) {
		if(!allowDropColumn()) {
			throw new IllegalArgumentException("Does not support drop column syntax");
		}
		if(!allowIfExistsModifyColumnSyntax()) {
			throw new IllegalArgumentException("Does not support if exists column syntax");
		}
		return "ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS " + columnName + ";";
	}
	
	@Override
	public String modColumnType(String tableName, String columnName, String dataType) {
		if(!allowRedefineColumn()) {
			throw new IllegalArgumentException("Does not support redefinition of column syntax");
		}
		return "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " " + dataType + ";";
	}
	
	@Override
	public String modColumnTypeWithDefault(String tableName, String columnName, String dataType, Object defualtValue) {
		if(!allowRedefineColumn()) {
			throw new IllegalArgumentException("Does not support redefinition of column syntax");
		}
		return "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " " + dataType + " " + defualtValue + ";";
	}
	
	@Override
	public String modColumnTypeIfExists(String tableName, String columnName, String dataType) {
		if(!allowRedefineColumn()) {
			throw new IllegalArgumentException("Does not support redefinition of column syntax");
		}
		if(!allowIfExistsModifyColumnSyntax()) {
			throw new IllegalArgumentException("Does not support if exists column syntax");
		}
		return "ALTER TABLE " + tableName + " ALTER COLUMN IF EXISTS " + columnName + " " + dataType + ";";
	}
	
	@Override
	public String modColumnTypeIfExistsWithDefault(String tableName, String columnName, String dataType, Object defualtValue) {
		if(!allowRedefineColumn()) {
			throw new IllegalArgumentException("Does not support redefinition of column syntax");
		}
		if(!allowIfExistsModifyColumnSyntax()) {
			throw new IllegalArgumentException("Does not support if exists column syntax");
		}
		return "ALTER TABLE " + tableName + " ALTER COLUMN IF EXISTS " + columnName + " " + dataType + " " + defualtValue + ";";
	}

	@Override
	public String createIndex(String indexName, String tableName, String column) {
		return "CREATE INDEX " + indexName + " ON " + tableName + "(" + column + ");";
	}

	@Override
	public String createIndex(String indexName, String tableName, Collection<String> columns) {
		StringBuilder builder = new StringBuilder();
		builder.append("CREATE INDEX ")
				.append(indexName)
				.append(" ON ")
				.append(tableName)
				.append("(");
		
		Iterator<String> colIt = columns.iterator();
		builder.append(colIt.next());
		while(colIt.hasNext()) {
			builder.append(", ").append(colIt.next());
		}
		builder.append(");");
		return builder.toString();
	}

	@Override
	public String createIndexIfNotExists(String indexName, String tableName, String column) {
		if(!allowIfExistsIndexSyntax()) {
			throw new IllegalArgumentException("Does not support if exists index syntax");
		}
		return "CREATE INDEX IF NOT EXISTS " + indexName + " ON " + tableName + "(" + column + ");";
	}

	@Override
	public String createIndexIfNotExists(String indexName, String tableName, Collection<String> columns) {
		if(!allowIfExistsIndexSyntax()) {
			throw new IllegalArgumentException("Does not support if exists index syntax");
		}
		StringBuilder builder = new StringBuilder();
		builder.append("CREATE INDEX IF NOT EXISTS ")
				.append(indexName)
				.append(" ON ")
				.append(tableName)
				.append("(");
		
		Iterator<String> colIt = columns.iterator();
		builder.append(colIt.next());
		while(colIt.hasNext()) {
			builder.append(", ").append(colIt.next());
		}
		builder.append(");");
		return builder.toString();
	}

	@Override
	public String dropIndex(String indexName, String tableName) {
		return "DROP INDEX " + indexName + ";";
	}

	@Override
	public String dropIndexIfExists(String indexName, String tableName) {
		if(!allowIfExistsIndexSyntax()) {
			throw new IllegalArgumentException("Does not support if exists index syntax");
		}
		return "DROP INDEX IF EXISTS " + indexName + ";";
	}

	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Query database scripts
	 */
	
	@Override
	public String tableExistsQuery(String tableName, String schema) {
		// there is no commonality that i have found for this
		throw new IllegalArgumentException("This operation does not have a standard across rdbms types. Please update the code for the specific RDBMS query util");
	}

	@Override
	public String columnDetailsQuery(String tableName, String columnName) {
		// there is no commonality that i have found for this
		throw new IllegalArgumentException("This operation does not have a standard across rdbms types. Please update the code for the specific RDBMS query util");
	}

	@Override
	public String getIndexList(String schema) {
		// there is no commonality that i have found for this
		throw new IllegalArgumentException("This operation does not have a standard across rdbms types. Please update the code for the specific RDBMS query util");
	}

	@Override
	public String getIndexDetails(String indexName, String tableName, String schema) {
		// there is no commonality that i have found for this
		throw new IllegalArgumentException("This operation does not have a standard across rdbms types. Please update the code for the specific RDBMS query util");
	}
	
	@Override
	public String allIndexForTableQuery(String tableName, String schema) {
		// there is no commonality that i have found for this
		throw new IllegalArgumentException("This operation does not have a standard across rdbms types. Please update the code for the specific RDBMS query util");
	}
}
