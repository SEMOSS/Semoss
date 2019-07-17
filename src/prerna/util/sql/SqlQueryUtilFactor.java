package prerna.util.sql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class SqlQueryUtilFactor {

	/**
	 * List of keywords based on what has been pushed into DIHelper
	 */
	private static Map<RdbmsTypeEnum, List<String>> keywordsMap = new HashMap<RdbmsTypeEnum, List<String>>();
	
	/**
	 * Get the appropriate query util class
	 * @param dbType
	 * @return
	 */
	public static AbstractSqlQueryUtil initialize(RdbmsTypeEnum dbType) {
		AbstractSqlQueryUtil queryUtil = null;
		if(dbType == RdbmsTypeEnum.H2_DB) {
			queryUtil = new H2QueryUtil();
		} else if(dbType == RdbmsTypeEnum.MARIADB){
			queryUtil = new MariaDbQueryUtil();
		} else if(dbType == RdbmsTypeEnum.SQLSERVER){
			queryUtil = new MicrosoftSqlServerUtil();
		} else if(dbType == RdbmsTypeEnum.MYSQL) {
			queryUtil = new MySQLQueryUtil();
		} else if(dbType == RdbmsTypeEnum.ORACLE) {
			queryUtil = new OracleQueryUtil();
		} else if(dbType == RdbmsTypeEnum.IMPALA) {
			queryUtil = new ImpalaQueryUtil();
		} else if(dbType == RdbmsTypeEnum.TIBCO) {
			queryUtil = new TibcoQueryUtil();
		} else if(dbType == RdbmsTypeEnum.SQLITE) {
			queryUtil = new SQLiteQueryUtil();
		} else if(dbType == RdbmsTypeEnum.SNOWFLAKE) {
			queryUtil = new SnowFlakeQueryUtil();
		}
		// base will work for most situations
		else {
			queryUtil = new AnsiSqlQueryUtil();
		}
		
		queryUtil.setDbType(dbType);
		queryUtil.setReservedWords(loadReservedWords(dbType));
		return queryUtil;
	}
	
	public static AbstractSqlQueryUtil initialize(RdbmsTypeEnum dbType, String connectionUrl, String username, String password) {
		AbstractSqlQueryUtil queryUtil = null;
		if(dbType == RdbmsTypeEnum.H2_DB) {
			queryUtil = new H2QueryUtil(connectionUrl, username, password);
		} else if(dbType == RdbmsTypeEnum.SQLSERVER){
			queryUtil = new MicrosoftSqlServerUtil(connectionUrl, username, password);
		} else if(dbType == RdbmsTypeEnum.MYSQL) {
			queryUtil = new MySQLQueryUtil(connectionUrl, username, password);
		}else if(dbType == RdbmsTypeEnum.MARIADB) {
			queryUtil = new MariaDbQueryUtil(connectionUrl, username, password);
		} else if(dbType == RdbmsTypeEnum.ORACLE) {
			queryUtil = new OracleQueryUtil(connectionUrl, username, password);
		} else if(dbType == RdbmsTypeEnum.IMPALA) {
			queryUtil = new ImpalaQueryUtil(connectionUrl, username, password);
		} else if(dbType == RdbmsTypeEnum.TIBCO) {
			queryUtil = new TibcoQueryUtil(connectionUrl, username, password);
		} else if(dbType == RdbmsTypeEnum.SQLITE) {
			queryUtil = new SQLiteQueryUtil(connectionUrl, username, password);
		} else if(dbType == RdbmsTypeEnum.SNOWFLAKE) {
			queryUtil = new SnowFlakeQueryUtil(connectionUrl, username, password);
		}
		// base will work for most situations
		else {
			queryUtil = new AnsiSqlQueryUtil(connectionUrl, username, password);
		}
		
		queryUtil.setDbType(dbType);
		queryUtil.setReservedWords(loadReservedWords(dbType));
		return queryUtil;
	}
	
	public static AbstractSqlQueryUtil initialize(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		AbstractSqlQueryUtil queryUtil = null;
		if(dbType == RdbmsTypeEnum.H2_DB) {
			queryUtil = new H2QueryUtil(dbType, hostname, port, schema, username, password);
		} else if(dbType == RdbmsTypeEnum.SQLSERVER){
			queryUtil = new MicrosoftSqlServerUtil(dbType, hostname, port, schema, username, password);
		} else if(dbType == RdbmsTypeEnum.MYSQL) {
			queryUtil = new MySQLQueryUtil(dbType, hostname, port, schema, username, password);
		} else if(dbType == RdbmsTypeEnum.ORACLE) {
			queryUtil = new OracleQueryUtil(dbType, hostname, port, schema, username, password);
		} else if(dbType == RdbmsTypeEnum.IMPALA) {
			queryUtil = new ImpalaQueryUtil(dbType, hostname, port, schema, username, password);
		} else if(dbType == RdbmsTypeEnum.TIBCO) {
			queryUtil = new TibcoQueryUtil(dbType, hostname, port, schema, username, password);
		} else if(dbType == RdbmsTypeEnum.SQLITE) {
			queryUtil = new SQLiteQueryUtil(dbType, hostname, port, schema, username, password);
		} else if(dbType == RdbmsTypeEnum.SNOWFLAKE) {
			queryUtil = new SnowFlakeQueryUtil(dbType, hostname, port, schema, username, password);
		}
		// base will work for most situations
		else {
			queryUtil = new AnsiSqlQueryUtil(dbType, hostname, port, schema, username, password);
		}
		
		queryUtil.setReservedWords(loadReservedWords(dbType));
		return queryUtil;
	}
	
	/**
	 * Load the reserved words from DIHelper (static - only load once per type)
	 * @param type
	 * @return
	 */
	private static List<String> loadReservedWords(RdbmsTypeEnum type) {
		if(keywordsMap.containsKey(type)) {
			return keywordsMap.get(type);
		}
		
		// see if it exists in dihelper and load
		String keywordsString = DIHelper.getInstance().getProperty(type.getLabel().toUpperCase() + Constants.KEYWORDS_SUFFIX);
		if(keywordsString != null) {
			List<String> keywordsList = new Vector<String>();
			// the string is comma delimited
			String[] words = keywordsString.split(",");
			for(String word : words) {
				// keep everything upper case for simplicity in comparisons
				keywordsList.add(word.toUpperCase());
			}
			keywordsMap.put(type, keywordsList);
		}
		
		return keywordsMap.get(type);
	}
}
