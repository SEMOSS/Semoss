package prerna.sablecc2.reactor.qs.source;

import java.sql.Connection;
import java.sql.SQLException;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.query.querystruct.HardQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class DirectJdbcConnectionReactor extends AbstractQueryStructReactor {
	
	public DirectJdbcConnectionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.CONNECTION_STRING_KEY.getKey(), 
				ReactorKeysEnum.DB_DRIVER_KEY.getKey(), ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey() };
	}

	@Override
	protected SelectQueryStruct createQueryStruct() {
		organizeKeys();
		String query = this.keyValue.get(this.keysToGet[0]);
		String connectionUrl = this.keyValue.get(this.keysToGet[1]);
		String driver = this.keyValue.get(this.keysToGet[2]);
		String userName = this.keyValue.get(this.keysToGet[3]);
		String password = this.keyValue.get(this.keysToGet[4]);
		
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
		qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		return qs;
	}

}
