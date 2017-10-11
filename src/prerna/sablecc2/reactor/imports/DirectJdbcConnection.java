package prerna.sablecc2.reactor.imports;

import java.sql.Connection;
import java.sql.SQLException;

import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
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

	@Override
	public NounMetadata execute() {
		String query = getQuery();
		String userName = getUserName();
		String password = getPassword();
		String driver = getDbDriver();
		String connectionUrl = getConnectionString();

		Connection con = null;
		try {
			con = RdbmsConnectionHelper.getConnection(connectionUrl, userName, password, driver);
		} catch (SQLException e1) {
			e1.printStackTrace();
			throw new IllegalArgumentException(e1.getMessage());
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
