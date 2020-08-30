package prerna.util.sql;

public class AsterQueryUtil extends AnsiSqlQueryUtil {

	AsterQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.ASTER);
	}
	
	AsterQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.ASTER);
	}
	
	AsterQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
		setDbType(RdbmsTypeEnum.ASTER);
	}
	
}
