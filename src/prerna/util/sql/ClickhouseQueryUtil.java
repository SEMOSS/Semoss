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
	
}
