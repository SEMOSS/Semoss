package prerna.util.sql;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class AsterQueryUtil extends AnsiSqlQueryUtil {

	AsterQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.ASTER);
	}
	
	AsterQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.ASTER);
	}
	
	AsterQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
		setDbType(RdbmsTypeEnum.ASTER);
	}
	
	@Override
	public String buildConnectionString(Map<String, Object> configMap) throws SQLException, RuntimeException {
		if(configMap.isEmpty()){
			throw new RuntimeException("Configuration Map is Empty.");
		}
		String urlPrefix = ((String) RdbmsTypeEnum.ASTER.getUrlPrefix()).toUpperCase();
		String hostname = ((String) configMap.get(AbstractSqlQueryUtil.HOSTNAME)).toUpperCase();
		String port = ((String) configMap.get(AbstractSqlQueryUtil.PORT)).toUpperCase();
		String schema = ((String) configMap.get(AbstractSqlQueryUtil.SCHEMA)).toUpperCase();
		String username = ((String) configMap.get(AbstractSqlQueryUtil.USERNAME)).toUpperCase();
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		String connectionString = urlPrefix+"://"+hostname+port+"/"+schema; 
		
		return connectionString;
	}

	@Override
	public String buildConnectionString(Properties prop) {
		if(prop == null){
			throw new RuntimeException("Properties ojbect is null");
		}
		String urlPrefix = ((String) RdbmsTypeEnum.ASTER.getUrlPrefix()).toUpperCase();
		String hostname = ((String) prop.getProperty(AbstractSqlQueryUtil.HOSTNAME)).toUpperCase();
		String port = ((String) prop.getProperty(AbstractSqlQueryUtil.PORT)).toUpperCase();
		String schema = ((String) prop.getProperty(AbstractSqlQueryUtil.SCHEMA)).toUpperCase();
		String username = ((String) prop.getProperty(AbstractSqlQueryUtil.USERNAME)).toUpperCase();
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		String connectionString = urlPrefix+"://"+hostname+port+"/"+schema; 
		
		return connectionString;
	}
	
}
