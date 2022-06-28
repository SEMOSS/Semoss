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
	
}
