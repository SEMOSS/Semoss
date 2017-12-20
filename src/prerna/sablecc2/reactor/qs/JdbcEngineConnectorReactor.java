package prerna.sablecc2.reactor.qs;

import java.sql.Connection;
import java.sql.SQLException;

import prerna.engine.impl.rdbms.ImpalaEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;

public class JdbcEngineConnectorReactor extends QueryStructReactor {
	
	public JdbcEngineConnectorReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.CONNECTION_STRING_KEY.getKey(), ReactorKeysEnum.DB_DRIVER_KEY.getKey(), ReactorKeysEnum.PASSWORD.getKey(), ReactorKeysEnum.USERNAME.getKey()};
	}
	
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
		GenRowStruct grs = this.store.getNoun(keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[0]);
	}

	private String getDbDriver() {
		GenRowStruct grs = this.store.getNoun(keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[1]);
	}

	private String getPassword() {
		GenRowStruct grs = this.store.getNoun(keysToGet[2]);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[2]);
	}

	private String getUserName() {
		GenRowStruct grs = this.store.getNoun(keysToGet[3]);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[3]);
	}
}
