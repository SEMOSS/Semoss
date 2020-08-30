package prerna.util.sql;

public class AthenaQueryUtil extends AnsiSqlQueryUtil {

	AthenaQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.ATHENA);
	}
	
	AthenaQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.ATHENA);
	}
	
	AthenaQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
}
