package prerna.util.sql;

public class DB2QueryUtil extends AnsiSqlQueryUtil {

	DB2QueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.DB2);
	}
	
	DB2QueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.DB2);
	}
	
}
