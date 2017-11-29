package prerna.sablecc2.reactor.qs;

import java.sql.Connection;
import java.sql.SQLException;

import prerna.engine.impl.rdbms.ImpalaEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;

public class JdbcEngineConnectorReactor extends QueryStructReactor {

	// constants used to get pixel inputs
	private static final String DB_DRIVER_KEY = "dbDriver";
	private static final String CONNECTION_STRING_KEY = "connectionString";
	private static final String USERNAME_KEY = "userName";
	private static final String PASSWORD_KEY = "password";
	
	@Override
	QueryStruct2 createQueryStruct() {
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
		
		//TODO: design pattern for this
		//TODO: design pattern for this
		//TODO: design pattern for this
		//TODO: design pattern for this
		RDBMSNativeEngine fakeEngine = null;
		if(driver.toLowerCase().contains("impala")) {
			fakeEngine = new ImpalaEngine();
		} else {
			fakeEngine = new RDBMSNativeEngine();
		}
		fakeEngine.setEngineName("FAKE_ENGINE");
		fakeEngine.setConnection(con);
		fakeEngine.setBasic(true);
		
		this.qs.setEngine(fakeEngine);
		this.qs.setQsType(QueryStruct2.QUERY_STRUCT_TYPE.ENGINE);
		return this.qs;
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
}
