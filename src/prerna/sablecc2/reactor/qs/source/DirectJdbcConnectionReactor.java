package prerna.sablecc2.reactor.qs.source;

import java.sql.Connection;
import java.sql.SQLException;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.query.querystruct.HardQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class DirectJdbcConnectionReactor extends AbstractQueryStructReactor {
	
	public DirectJdbcConnectionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.CONNECTION_STRING_KEY.getKey(), ReactorKeysEnum.DB_DRIVER_KEY.getKey(), ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey() };
	}

	@Override
	protected QueryStruct2 createQueryStruct() {
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
		
		RDBMSNativeEngine fakeEngine = new RDBMSNativeEngine();
		fakeEngine.setEngineName("DIRECT_ENGINE_CONNECTION");
		fakeEngine.setConnection(con);
		fakeEngine.setBasic(true);
		
		HardQueryStruct qs = new HardQueryStruct();
		qs.setQuery(query);
		qs.setEngine(fakeEngine);
		qs.setQsType(QueryStruct2.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		return qs;
	}
	
	/*
	 * Pixel inputs
	 */
	private String getConnectionString() {
		GenRowStruct grs = this.store.getNoun(keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[1]);
	}

	private String getDbDriver() {
		GenRowStruct grs = this.store.getNoun(keysToGet[2]);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[2]);
	}

	private String getPassword() {
		GenRowStruct grs = this.store.getNoun(keysToGet[4]);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[4]);
	}

	private String getUserName() {
		GenRowStruct grs = this.store.getNoun(keysToGet[3]);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[3]);
	}

	private String getQuery() {
		GenRowStruct grs = this.store.getNoun(keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[0]);
	}

}
