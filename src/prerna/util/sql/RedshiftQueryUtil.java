package prerna.util.sql;

public class RedshiftQueryUtil extends AnsiSqlQueryUtil {

	RedshiftQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.REDSHIFT);
	}
	
	RedshiftQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.REDSHIFT);
	}
	
	RedshiftQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
}
