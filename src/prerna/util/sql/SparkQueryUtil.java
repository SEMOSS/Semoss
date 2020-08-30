package prerna.util.sql;

public class SparkQueryUtil extends AnsiSqlQueryUtil {

	SparkQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.SPARK);
	}
	
	SparkQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.SPARK);
	}
	
	SparkQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
}
