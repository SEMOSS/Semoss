package prerna.util.sql;

public class OracleQueryUtil extends AnsiSqlQueryUtil {
	
	OracleQueryUtil() {
		super();
	}
	
	OracleQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
	}
	
	OracleQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}

	@Override
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {
		if(offset > 0) {
			query = query.append(" OFFSET " + offset + " ROWS ");
		}
		if(limit > 0) {
			query = query.append(" FETCH NEXT " + limit+" ROWS ONLY ");
		}
		return query;
	}
	
	@Override
	public boolean allowIfExistsModifyColumnSyntax() {
		return false;
	}
	
	@Override
	public String modColumnType(String tableName, String columnName, String dataType) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "ALTER TABLE " + tableName + " MODIFY " + columnName + " " + dataType + ";";
	}
	
	@Override
	public String dropIndex(String indexName, String tableName) {
		return "DROP INDEX " + indexName;
	}
}
