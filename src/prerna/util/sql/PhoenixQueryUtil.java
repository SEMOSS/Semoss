package prerna.util.sql;

public class PhoenixQueryUtil extends AnsiSqlQueryUtil {

	PhoenixQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.PHOENIX);
	}
	
	PhoenixQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.PHOENIX);
	}
	
}
