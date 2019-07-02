package prerna.util.sql;

public class ImpalaQueryUtil extends AnsiSqlQueryUtil {
	
	ImpalaQueryUtil() {
		super();
	}
	
	ImpalaQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
	}
	
	ImpalaQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}

}
