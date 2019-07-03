package prerna.util.sql;

public enum RdbmsTypeEnum {

	ASTER("ASTER_DB", "com.asterdata.ncluster.jdbc.core.NClusterJDBCDriver", "jdbc:ncluster"),
	CASSANDRA("CASSANDRA", "com.github.cassandra.jdbc.CassandraDriver", "jdbc:cassandra"),
	DB2("DB2", "com.ibm.db2.jcc.DB2Driver", "jdbc:db2"),
	DERBY("DERBY", "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby"),
	H2_DB("H2_DB", "org.h2.Driver", "jdbc:h2"),
	SQLITE("SQLITE", "org.sqlite.JDBC", "jdbc:sqlite"),
	IMPALA("IMPALA", "com.cloudera.impala.jdbc4.Driver", "jdbc:impala"),
	REDSHIFT("REDSHIFT", "com.amazon.redshift.jdbc.Driver", "jdbc:redshift"),
	MARIADB("MARIA_DB", "org.mariadb.jdbc.Driver", "jdbc:mariadb"),
	MYSQL("MYSQL", "com.mysql.jdbc.Driver", "jdbc:mysql"),
	ORACLE("ORACLE", "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin"),
	PHOENIX("PHOENIX", "org.apache.phoenix.jdbc.PhoenixDriver", "jdbc:phoenix"),
	POSTGRES("POSTGRES", "org.postgresql.Driver", "jdbc:postgresql"),
	SAP_HANA("SAP_HANA", "com.sap.db.jdbc.Driver", "jdbc:sap"),
	SNOWFLAKE("SNOWFLAKE","net.snowflake.client.jdbc.SnowflakeDriver", "jdbc:snowflake"),
	SQLSERVER("SQL_SERVER", "com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver"),
	TERADATA("TERADATA", "com.teradata.jdbc.TeraDriver", "jdbc:teradata"),
	TIBCO("TIBCO", "cs.jdbc.driver.CompositeDriver", "jdbc:compositesw:dbapi");
	
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
