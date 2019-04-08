package prerna.util.sql;

public enum RdbmsTypeEnum {

	ASTER("ASTER_DB", "com.asterdata.ncluster.jdbc.core.NClusterJDBCDriver"),
	CASSANDRA("CASSANDRA", "com.github.adejanovski.cassandra.jdbc.CassandraDriver"),
	DB2("DB2", "com.ibm.db2.jcc.DB2Driver"),
	DERBY("DERBY", "org.apache.derby.jdbc.EmbeddedDriver"),
	H2_DB("H2_DB", "org.h2.Driver"),
	IMPALA("IMPALA", "com.cloudera.impala.jdbc4.Driver"),
	REDSHIFT("REDSHIFT", "com.amazon.redshift.jdbc.Driver"),
	MARIADB("MARIA_DB", "org.mariadb.jdbc.Driver"),
	MYSQL("MYSQL", "com.mysql.jdbc.Driver"),
	ORACLE("ORACLE", "oracle.jdbc.driver.OracleDriver"),
	PHOENIX("PHOENIX", "org.apache.phoenix.jdbc.PhoenixDriver"),
	POSTGRES("POSTGRES", "org.postgresql.Driver"),
	SAP_HANA("SAP_HANA", "com.sap.db.jdbc.Driver"),
	SNOWFLAKE("SNOWFLAKE","net.snowflake.client.jdbc.SnowflakeDriver"),
	SQLSERVER("SQL_SERVER", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
	TERADATA("TERADATA", "com.teradasta.jdbc.TeraDriver"),
	TIBCO("TIBCO", "cs.jdbc.driver.CompositeDriver"),
	SQLITE("SQLITE", "org.sqlite.JDBC");
	
	private String label;
	private String driver;
	
	RdbmsTypeEnum(String label, String driver) {
		this.label = label;
		this.driver = driver;
	}
	
	public String getLabel() {
		return this.label;
	}
	
	public String getDriver() {
		return this.driver;
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
	
}
