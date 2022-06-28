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
	
}
