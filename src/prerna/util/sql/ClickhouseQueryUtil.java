package prerna.util.sql;

public class ClickhouseQueryUtil extends AnsiSqlQueryUtil {

	ClickhouseQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.CLICKHOUSE);
	}
	
	ClickhouseQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.CLICKHOUSE);
	}
	
	ClickhouseQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
}
