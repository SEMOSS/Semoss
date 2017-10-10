package prerna.sablecc2.reactor.imports;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import prerna.ds.TinkerMetaHelper;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.sablecc.PKQLRunner;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.Task;
import prerna.sablecc2.reactor.AbstractReactor;

public class DirectJdbcConnection extends AbstractReactor {

	// constants used to get pixel inputs
	public static final String QUERY_KEY = "query";
	public static final String DB_DRIVER_KEY = "dbDriver";
	public static final String CONNECTION_STRING_KEY = "connectionString";
	public static final String USERNAME_KEY = "userName";
	public static final String PASSWORD_KEY = "password";

	private static final String ASTER = "ASTER_DB";
	private static final String ASTER_DRIVER = "com.asterdata.ncluster.jdbc.core.NClusterJDBCDriver";
	private static final String CASSANDRA = "CASSANDRA";
	private static final String CASSANDRA_DRIVER = "com.github.adejanovski.cassandra.jdbc.CassandraDriver";
	private static final String DB2 = "DB2";
	private static final String DB2_DRIVER = "com.ibm.db2.jcc.DB2Driver";
	private static final String DERBY = "DERBY";
	private static final String DERBY_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String H2 = "H2_DB";
	private static final String H2_DRIVER = "org.h2.Driver";
	private static final String IMPALA = "IMPALA";
	private static final String IMPALA_DRIVER = "com.cloudera.impala.jdbc3.Driver";
	private static final String MARIADB = "MARIA_DB";
	private static final String MARIADB_DRIVER = "org.mariadb.jdbc.Driver";
	private static final String MYSQL = "MYSQL";
	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private static final String ORACLE = "ORACLE";
	private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private static final String PHOENIX = "PHOENIX";
	private static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
	private static final String POSTGRES = "POSTGRES";
	private static final String POSTGRES_DRIVER = "org.postgresql.Driver";
	private static final String SAP_HANA = "SAP_HANA";
	private static final String SAP_HANA_DRIVER = "com.sap.db.jdbc.Driver";
	private static final String SQLSERVER = "SQL_SERVER";
	private static final String SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private static final String TERADATA = "TERADATA";
	private static final String TERADATA_DRIVER = "com.teradata.jdbc.TeraDriver";

	@Override
	public NounMetadata execute() {
		String query = getQuery();
		String userName = getUserName();
		String password = getPassword();
		String driver = getDbDriver();
		String connectionUrl = getConnectionString();

		try {
			if (driver.equalsIgnoreCase(ASTER)) {
				Class.forName(ASTER_DRIVER);
			} else if (driver.equalsIgnoreCase(CASSANDRA)) {
				Class.forName(CASSANDRA_DRIVER);
			} else if (driver.equalsIgnoreCase(DB2)) {
				Class.forName(DB2_DRIVER);
			} else if (driver.equalsIgnoreCase(DERBY)) {
				Class.forName(DERBY_DRIVER);
			} else if (driver.equalsIgnoreCase(H2)) {
				Class.forName(H2_DRIVER);
			} else if (driver.equalsIgnoreCase(IMPALA)) {
				Class.forName(IMPALA_DRIVER);
			} else if (driver.equalsIgnoreCase(MARIADB)) {
				Class.forName(MARIADB_DRIVER);
			} else if (driver.equalsIgnoreCase(MYSQL)) {
				Class.forName(MYSQL_DRIVER);
			} else if (driver.equalsIgnoreCase(ORACLE)) {
				Class.forName(ORACLE_DRIVER);
			} else if (driver.equalsIgnoreCase(PHOENIX)) {
				Class.forName(PHOENIX_DRIVER);
			} else if (driver.equalsIgnoreCase(POSTGRES)) {
				Class.forName(POSTGRES_DRIVER);
			} else if (driver.equalsIgnoreCase(SAP_HANA)) {
				Class.forName(SAP_HANA_DRIVER);
			} else if (driver.equalsIgnoreCase(SQLSERVER)) {
				Class.forName(SQLSERVER_DRIVER);
			} else if (driver.equalsIgnoreCase(TERADATA)) {
				Class.forName(TERADATA_DRIVER);
			} else {
				throw new IllegalArgumentException("Invalid driver");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Unable to find driver for engine type");
		}
		
		// create the iterator
		Connection con;
		try {
			if (userName == null || password == null) {
				con = DriverManager.getConnection(connectionUrl);
			} else {
				con = DriverManager.getConnection(connectionUrl, userName, password);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}

		RawRDBMSSelectWrapper it = new RawRDBMSSelectWrapper();
		try {
			it.setCloseConenctionAfterExecution(true);
			it.directExecutionViaConnection(con, query, true);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		
		// create task
		Task task = new Task(it);
		return new NounMetadata(task, PixelDataType.TASK, PixelOperationType.TASK);
	}

	/*
	 * Pixel inputs
	 */
	private String getConnectionString() {
		GenRowStruct grs = this.store.getNoun(CONNECTION_STRING_KEY);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + CONNECTION_STRING_KEY);
	}

	private String getDbDriver() {
		GenRowStruct grs = this.store.getNoun(DB_DRIVER_KEY);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + DB_DRIVER_KEY);
	}

	private String getPassword() {
		GenRowStruct grs = this.store.getNoun(PASSWORD_KEY);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + PASSWORD_KEY);
	}

	private String getUserName() {
		GenRowStruct grs = this.store.getNoun(USERNAME_KEY);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + USERNAME_KEY);
	}

	private String getQuery() {
		GenRowStruct grs = this.store.getNoun(QUERY_KEY);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + QUERY_KEY);
	}

}
