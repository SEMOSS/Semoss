package prerna.util.sql;

public class DerbyQueryUtil extends AnsiSqlQueryUtil {

	DerbyQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.DERBY);
	}
	
	DerbyQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.DERBY);
	}
	
	DerbyQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
}
