package prerna.util.sql;

public class TibcoQueryUtil extends AnsiSqlQueryUtil {

	TibcoQueryUtil() {
		super();
	}
	
	TibcoQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
	}
	
	TibcoQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
	@Override
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {
		if(offset > 0) {
			query = query.append(" OFFSET " + offset + " ROWS ");
		}
		if(limit > 0) {
			query = query.append(" FETCH NEXT " + limit + " ROWS ONLY ");
		}
		return query;
	}
}
