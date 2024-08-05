package prerna.util.sql;

import static prerna.util.sql.RdbmsTypeEnum.ASTER;
import static prerna.util.sql.RdbmsTypeEnum.ATHENA;
import static prerna.util.sql.RdbmsTypeEnum.BIG_QUERY;
import static prerna.util.sql.RdbmsTypeEnum.CASSANDRA;
import static prerna.util.sql.RdbmsTypeEnum.CLICKHOUSE;
import static prerna.util.sql.RdbmsTypeEnum.DATABRICKS;
import static prerna.util.sql.RdbmsTypeEnum.DB2;
import static prerna.util.sql.RdbmsTypeEnum.DERBY;
import static prerna.util.sql.RdbmsTypeEnum.ELASTIC_SEARCH;
import static prerna.util.sql.RdbmsTypeEnum.H2_DB;
import static prerna.util.sql.RdbmsTypeEnum.H2_V2_DB;
import static prerna.util.sql.RdbmsTypeEnum.HIVE;
import static prerna.util.sql.RdbmsTypeEnum.IMPALA;
import static prerna.util.sql.RdbmsTypeEnum.MARIADB;
import static prerna.util.sql.RdbmsTypeEnum.MYSQL;
import static prerna.util.sql.RdbmsTypeEnum.OPEN_SEARCH;
import static prerna.util.sql.RdbmsTypeEnum.ORACLE;
import static prerna.util.sql.RdbmsTypeEnum.PHOENIX;
import static prerna.util.sql.RdbmsTypeEnum.POSTGRES;
import static prerna.util.sql.RdbmsTypeEnum.REDSHIFT;
import static prerna.util.sql.RdbmsTypeEnum.SAP_HANA;
import static prerna.util.sql.RdbmsTypeEnum.SEMOSS;
import static prerna.util.sql.RdbmsTypeEnum.SNOWFLAKE;
import static prerna.util.sql.RdbmsTypeEnum.SPARK;
import static prerna.util.sql.RdbmsTypeEnum.SQLITE;
import static prerna.util.sql.RdbmsTypeEnum.SQL_SERVER;
import static prerna.util.sql.RdbmsTypeEnum.SYNAPSE;
import static prerna.util.sql.RdbmsTypeEnum.TERADATA;
import static prerna.util.sql.RdbmsTypeEnum.TIBCO;
import static prerna.util.sql.RdbmsTypeEnum.TRINO;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.util.Constants;
import prerna.util.Utility;

public class SqlQueryUtilFactory {

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
		if(dbType == ASTER) {
			queryUtil = new AsterQueryUtil();
		} else if(dbType == ATHENA) {
			queryUtil = new AthenaQueryUtil();
		} else if(dbType == BIG_QUERY) {
			queryUtil = new BigQueryQueryUtil();
		} else if(dbType == CASSANDRA) {
			queryUtil = new CassandraQueryUtil();
		} else if(dbType == CLICKHOUSE) {
			queryUtil = new ClickhouseQueryUtil();
		} else if(dbType == DATABRICKS) {
			queryUtil = new DatabricksQueryUtil();
		} else if(dbType == DB2) {
			queryUtil = new DB2QueryUtil();
		} else if(dbType == DERBY) {
			queryUtil = new DerbyQueryUtil();
		} else if(dbType == ELASTIC_SEARCH) {
			queryUtil = new ElasticSearchQueryUtil();
		} else if(dbType == H2_DB) {
			queryUtil = new H2QueryUtil();
		} else if(dbType == H2_V2_DB) {
			queryUtil = new H2v2QueryUtil();
		} else if(dbType == SQLITE) {
			queryUtil = new SQLiteQueryUtil();
		} else if(dbType == HIVE) {
			queryUtil = new HiveQueryUtil();
		} else if(dbType == IMPALA) {
			queryUtil = new ImpalaQueryUtil();
		} else if(dbType == REDSHIFT) {
			queryUtil = new RedshiftQueryUtil();
		} else if(dbType == MARIADB) {
			queryUtil = new MariaDbQueryUtil();
		} else if(dbType == MYSQL) {
			queryUtil = new MySQLQueryUtil();
		} else if(dbType == OPEN_SEARCH) {
			queryUtil = new OpenSearchQueryUtil();
		} else if(dbType == ORACLE) {
			queryUtil = new OracleQueryUtil();
		} else if(dbType == PHOENIX) {
			queryUtil = new PhoenixQueryUtil();
		} else if(dbType == POSTGRES) {
			queryUtil = new PostgresQueryUtil();
		} else if(dbType == SAP_HANA) {
			queryUtil = new SAPHanaQueryUtil();
		} else if(dbType == SPARK) {
			queryUtil = new SparkQueryUtil();
		} else if(dbType == SNOWFLAKE) {
			queryUtil = new SnowFlakeQueryUtil();
		} else if(dbType == SYNAPSE) {
			queryUtil = new SynapseQueryUtil();
		}  else if(dbType == SQL_SERVER) {
			queryUtil = new MicrosoftSqlServerQueryUtil();
		} else if(dbType == TERADATA) {
			queryUtil = new TeradataQueryUtil();
		} else if(dbType == TIBCO) {
			queryUtil = new TibcoQueryUtil();
		} else if(dbType == TRINO) {
			queryUtil = new TrinoQueryUtil();
		} else if(dbType == SEMOSS) {
			queryUtil = new SEMOSSQueryUtil();
		} else {
			throw new IllegalArgumentException("Unknown DB Type. Please define a query util for the DB Type " + dbType);
		}
			
		queryUtil.setReservedWords(loadReservedWords(dbType));
		return queryUtil;
	}
	
	public static AbstractSqlQueryUtil initialize(RdbmsTypeEnum dbType, String connectionUrl, String username, String password) {
		AbstractSqlQueryUtil queryUtil = null;
		if(dbType == ASTER) {
			queryUtil = new AsterQueryUtil(connectionUrl, username, password);
		} else if(dbType == ATHENA) {
			queryUtil = new AthenaQueryUtil(connectionUrl, username, password);
		} else if(dbType == BIG_QUERY) {
			queryUtil = new BigQueryQueryUtil(connectionUrl, username, password);
		} else if(dbType == CASSANDRA) {
			queryUtil = new CassandraQueryUtil(connectionUrl, username, password);
		} else if(dbType == CLICKHOUSE) {
			queryUtil = new ClickhouseQueryUtil(connectionUrl, username, password);
		} else if(dbType == DATABRICKS) {
			queryUtil = new DatabricksQueryUtil(connectionUrl, username, password);
		} else if(dbType == DB2) {
			queryUtil = new DB2QueryUtil(connectionUrl, username, password);
		} else if(dbType == DERBY) {
			queryUtil = new DerbyQueryUtil(connectionUrl, username, password);
		} else if(dbType == ELASTIC_SEARCH) {
			queryUtil = new ElasticSearchQueryUtil(connectionUrl, username, password);
		} else if(dbType == H2_DB) {
			queryUtil = new H2QueryUtil(connectionUrl, username, password);
		} else if(dbType == H2_V2_DB) {
			queryUtil = new H2v2QueryUtil(connectionUrl, username, password);
		} else if(dbType == SQLITE) {
			queryUtil = new SQLiteQueryUtil(connectionUrl, username, password);
		} else if(dbType == HIVE) {
			queryUtil = new HiveQueryUtil(connectionUrl, username, password);
		} else if(dbType == IMPALA) {
			queryUtil = new ImpalaQueryUtil(connectionUrl, username, password);
		} else if(dbType == REDSHIFT) {
			queryUtil = new RedshiftQueryUtil(connectionUrl, username, password);
		} else if(dbType == MARIADB) {
			queryUtil = new MariaDbQueryUtil(connectionUrl, username, password);
		} else if(dbType == MYSQL) {
			queryUtil = new MySQLQueryUtil(connectionUrl, username, password);
		} else if(dbType == OPEN_SEARCH) {
			queryUtil = new OpenSearchQueryUtil(connectionUrl, username, password);
		} else if(dbType == ORACLE) {
			queryUtil = new OracleQueryUtil(connectionUrl, username, password);
		} else if(dbType == PHOENIX) {
			queryUtil = new PhoenixQueryUtil(connectionUrl, username, password);
		} else if(dbType == POSTGRES) {
			queryUtil = new PostgresQueryUtil(connectionUrl, username, password);
		} else if(dbType == SAP_HANA) {
			queryUtil = new SAPHanaQueryUtil(connectionUrl, username, password);
		} else if(dbType == SPARK) {
			queryUtil = new SparkQueryUtil(connectionUrl, username, password);
		} else if(dbType == SNOWFLAKE) {
			queryUtil = new SnowFlakeQueryUtil(connectionUrl, username, password);
		} else if(dbType == SYNAPSE) {
			queryUtil = new SynapseQueryUtil(connectionUrl, username, password);
		} else if(dbType == SQL_SERVER) {
			queryUtil = new MicrosoftSqlServerQueryUtil(connectionUrl, username, password);
		} else if(dbType == TERADATA) {
			queryUtil = new TeradataQueryUtil(connectionUrl, username, password);
		} else if(dbType == TIBCO) {
			queryUtil = new TibcoQueryUtil(connectionUrl, username, password);
		} else if(dbType == TRINO) {
			queryUtil = new TrinoQueryUtil(connectionUrl, username, password);
		} else if(dbType == SEMOSS) {
			queryUtil = new SEMOSSQueryUtil(connectionUrl, username, password);
		} else {
			throw new IllegalArgumentException("Unknown DB Type. Please define a query util for the DB Type " + dbType);
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
		
		String keywordsString = Utility.getDIHelperProperty(type.getLabel().toUpperCase() + Constants.KEYWORDS_SUFFIX);
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
	
	private SqlQueryUtilFactory() {
		
	}
}
