package prerna.util.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.MicrosoftSqlServerInterpreter;

public class MicrosoftSqlServerUtil extends AnsiSqlQueryUtil {
	
	MicrosoftSqlServerUtil() {
		super();
	}
	
	MicrosoftSqlServerUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
	}
	
	MicrosoftSqlServerUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
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
	public String removeDuplicatesFromTable(String tableName, String fullColumnNameList){
		return "SELECT DISTINCT " + fullColumnNameList 
					+ " INTO " + tableName + "_TEMP " 
					+ " FROM " + tableName + " WHERE " + tableName 
					+ " IS NOT NULL AND LTRIM(RTRIM(" + tableName + ")) <> ''";
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	@Override
	public boolean allowsIfExistsTableSyntax() {
		return false;
	}
	
	@Override
	public String tableExistsQuery(String tableName, String schema) {
		return "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='" + schema + "' AND TABLE_NAME='" + tableName +"'";
	}
	
	@Override
	public String alterTableName(String tableName, String newTableName) {
		return "sp_reanme '" + tableName + "', '" + newTableName + "';";
	}
	
	@Override
	public String dropIndex(String indexName, String tableName) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		return "DROP INDEX " + tableName + "." + indexName + ";";
	}

}
