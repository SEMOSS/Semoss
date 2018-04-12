package prerna.sablecc2.reactor.qs.source;

import java.sql.Connection;
import java.sql.SQLException;

import prerna.engine.impl.rdbms.ImpalaEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class JdbcEngineConnectorReactor extends AbstractQueryStructReactor {
	
	public JdbcEngineConnectorReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.CONNECTION_STRING_KEY.getKey(), ReactorKeysEnum.DB_DRIVER_KEY.getKey(), 
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}
	
	@Override
	protected QueryStruct2 createQueryStruct() {
		organizeKeys();
		String connectionUrl = this.keyValue.get(this.keysToGet[0]);
		String driver = this.keyValue.get(this.keysToGet[1]);
		String userName = this.keyValue.get(this.keysToGet[2]);
		String password = this.keyValue.get(this.keysToGet[3]);

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
	
}
