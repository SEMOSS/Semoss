package prerna.sablecc2.reactor.qs.source;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import prerna.engine.impl.rdbms.ImpalaEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.TemporalEngineHardQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class JdbcSourceReactor extends AbstractQueryStructReactor {
	
	public JdbcSourceReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.CONNECTION_STRING_KEY.getKey(), ReactorKeysEnum.DB_DRIVER_KEY.getKey(), 
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		organizeKeys();
		String connectionUrl = this.keyValue.get(this.keysToGet[0]);
		String driver = this.keyValue.get(this.keysToGet[1]);
		String userName = this.keyValue.get(this.keysToGet[2]);
		String password = this.keyValue.get(this.keysToGet[3]);

		// need to maintain what the FE passed to create this 
		Map<String, Object> config = new HashMap<String, Object>();
		config.put(this.keysToGet[0], connectionUrl);
		config.put(this.keysToGet[1], driver);
		config.put(this.keysToGet[2], userName);
		config.put(this.keysToGet[3], password);

		Connection con = null;
		try {
			con = RdbmsConnectionHelper.getConnection(connectionUrl, userName, password, driver);
		} catch (SQLException e1) {
			e1.printStackTrace();
			throw new IllegalArgumentException(e1.getMessage());
		}
		
		RDBMSNativeEngine temporalEngine = null;
		if(driver.toLowerCase().contains("impala")) {
			temporalEngine = new ImpalaEngine();
		} else {
			temporalEngine = new RDBMSNativeEngine();
		}
		temporalEngine.setEngineId("FAKE_ENGINE");
		temporalEngine.setConnection(con);
		temporalEngine.setBasic(true);
		
		TemporalEngineHardQueryStruct qs = new TemporalEngineHardQueryStruct();
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY);
		qs.setConfig(config);
		qs.setEngine(temporalEngine);
		return qs;
	}
	
}
