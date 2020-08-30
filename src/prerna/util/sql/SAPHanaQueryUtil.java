package prerna.util.sql;

public class SAPHanaQueryUtil extends AnsiSqlQueryUtil {

	SAPHanaQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.SAP_HANA);
	}
	
	SAPHanaQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.SAP_HANA);
	}
	
	SAPHanaQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
}
