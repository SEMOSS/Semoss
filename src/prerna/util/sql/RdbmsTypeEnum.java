package prerna.util.sql;

public enum RdbmsTypeEnum {

	ASTER("ASTER_DB", "com.asterdata.ncluster.jdbc.core.NClusterJDBCDriver", "jdbc:ncluster"),
	ATHENA("ATHENA","com.simba.athena.jdbc.Driver", "jdbc:awsathena"),
	BIG_QUERY("BIG_QUERY","com.simba.googlebigquery.jdbc42.Driver", "jdbc:bigquery"),
	CASSANDRA("CASSANDRA", "com.simba.cassandra.jdbc42.Driver", "jdbc:cassandra"),
	CLICKHOUSE("CLICKHOUSE", "ru.yandex.clickhouse.ClickHouseDriver", "jdbc:clickhouse"),
	DATABRICKS("DATABRICKS", "com.databricks.client.jdbc.Driver", "jdbc:databricks"),
	DB2("DB2", "com.ibm.db2.jcc.DB2Driver", "jdbc:db2"),
	DERBY("DERBY", "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby"),
	ELASTIC_SEARCH("ELASTIC_SEARCH", "org.elasticsearch.xpack.sql.jdbc.EsDriver", "jdbc:es"),
	H2_DB("H2_DB", "org.h2.Driver", "jdbc:h2"),
	H2_V2_DB("H2_V2_DB", "org.h2.Driver", "jdbc:h2"),
	HIVE("HIVE","org.apache.hive.jdbc.HiveDriver","jdbc:hive2"),
	IMPALA("IMPALA", "com.cloudera.impala.jdbc4.Driver", "jdbc:impala"),
	REDSHIFT("REDSHIFT", "com.amazon.redshift.jdbc.Driver", "jdbc:redshift"),
	MARIADB("MARIA_DB", "org.mariadb.jdbc.Driver", "jdbc:mariadb"),
	MYSQL("MYSQL", "com.mysql.cj.jdbc.Driver", "jdbc:mysql"),
	OPEN_SEARCH("OPEN_SEARCH", "org.opensearch.jdbc.Driver", "jdbc:opensearch"),
	ORACLE("ORACLE", "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin"),
	PHOENIX("PHOENIX", "org.apache.phoenix.jdbc.PhoenixDriver", "jdbc:phoenix"),
	POSTGRES("POSTGRES", "org.postgresql.Driver", "jdbc:postgresql"),
	SAP_HANA("SAP_HANA", "com.sap.db.jdbc.Driver", "jdbc:sap"),
	SPARK("SPARK", "com.simba.spark.jdbc41.Driver", "jdbc:spark"),
	SQLITE("SQLITE", "org.sqlite.JDBC", "jdbc:sqlite"),
	SNOWFLAKE("SNOWFLAKE","net.snowflake.client.jdbc.SnowflakeDriver", "jdbc:snowflake"),
	SYNAPSE("SYNAPSE", "com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver"),
	SQL_SERVER("SQL_SERVER", "com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver"),
	TERADATA("TERADATA", "com.teradata.jdbc.TeraDriver", "jdbc:teradata"),
	TIBCO("TIBCO", "cs.jdbc.driver.CompositeDriver", "jdbc:compositesw:dbapi"),
	TRINO("TRINO", "io.trino.jdbc.TrinoDriver", "jdbc:trino"),
	// SEMOSS to your SEMOSS
	SEMOSS("SEMOSS", "prerna.jdbc.SMSSDriver", "jdbc:smss");

	private String label;
	private String driver;
	private String urlPrefix;
	
	RdbmsTypeEnum(String label, String driver, String urlPrefix) {
		this.label = label;
		this.driver = driver;
		this.urlPrefix = urlPrefix;
	}
	
	public String getLabel() {
		return this.label;
	}
	
	public String getDriver() {
		return this.driver;
	}
	
	public String getUrlPrefix() {
		return this.urlPrefix;
	}
	
	/**
	 * Get the driver based on the string
	 * @param type
	 * @return
	 */
	public static String getDriverFromString(String type) {
		type = type.toUpperCase();
		RdbmsTypeEnum foundType = null;
		try {
			foundType = RdbmsTypeEnum.valueOf(type);
		} catch(IllegalArgumentException e) {
			// ignore
		}
		// if we found a type
		// return the driver
		if(foundType != null) {
			return foundType.getDriver();
		}
		
		// loop through and see if passed in is a label
		for(RdbmsTypeEnum rdbmsType : RdbmsTypeEnum.values()) {
			if(type.equals(rdbmsType.label)) {
				return rdbmsType.getDriver();
			}
		}
		
		return null;
	}
	
	/**
	 * Get the enum based on the type
	 * @param type
	 * @return
	 */
	public static RdbmsTypeEnum getEnumFromString(String type) {
		type = type.toUpperCase();
		RdbmsTypeEnum foundType = null;
		try {
			foundType = RdbmsTypeEnum.valueOf(type);
		} catch(IllegalArgumentException e) {
			// ignore
		}
		// if we found a type
		// return the driver
		if(foundType != null) {
			return foundType;
		}
		
		// loop through and see if passed in is a label
		for(RdbmsTypeEnum rdbmsType : RdbmsTypeEnum.values()) {
			if(type.equals(rdbmsType.label)) {
				return rdbmsType;
			}
		}
		
		return null;
	}
	
	/**
	 * Get the enum from the driver
	 * @param driver
	 * @return
	 */
	public static RdbmsTypeEnum getEnumFromDriver(String driver) {
		for(RdbmsTypeEnum rdbmsType : RdbmsTypeEnum.values()) {
			if(driver.equalsIgnoreCase(rdbmsType.driver)) {
				return rdbmsType;
			}
		}
		
		return null;
	}
	
}
