package prerna.util.sql;

public class CassandraQueryUtil extends AnsiSqlQueryUtil {

	CassandraQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.CASSANDRA);
	}
	
	CassandraQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.CASSANDRA);
	}
	
	CassandraQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
}
