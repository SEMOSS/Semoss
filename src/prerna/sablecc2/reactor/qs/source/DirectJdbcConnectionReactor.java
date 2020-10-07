package prerna.sablecc2.reactor.qs.source;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import prerna.engine.impl.rdbms.ImpalaEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.TemporalEngineHardQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.EmbeddedRoutineReactor;
import prerna.sablecc2.reactor.EmbeddedScriptReactor;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

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
		
		// need to maintain what the FE passed to create this 
		Map<String, Object> config = new HashMap<String, Object>();
		config.put(this.keysToGet[0], connectionUrl);
		config.put(this.keysToGet[1], driver);
		config.put(this.keysToGet[2], userName);
		config.put(this.keysToGet[3], password);
		
		if(driver == null) {
			throw new IllegalArgumentException("Must pass in the rdbms type");
		}
		RdbmsTypeEnum dbType = RdbmsTypeEnum.getEnumFromString(driver);
		if(dbType == null) {
			// try one more time
			dbType =  RdbmsTypeEnum.getEnumFromDriver(driver);
			if(dbType == null) {
				throw new IllegalArgumentException("Unable to find driver for rdbms type = " + driver);
			}
		}
		
		Connection con = null;
		try {
			con = AbstractSqlQueryUtil.makeConnection(dbType, connectionUrl, userName, password);
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
		if(query != null && !query.isEmpty()) {
			query = Utility.decodeURIComponent(query);
			qs.setQuery(query);
		}
		return qs;
	}
	
	@Override
	public void mergeUp() {
		// merge this reactor into the parent reactor
		init();
		createQueryStructPlan();
		if(parentReactor != null) {
			// this is only called lazy
			// have to init to set the qs
			// to them add to the parent
			NounMetadata data = new NounMetadata(this.qs, PixelDataType.QUERY_STRUCT);
	    	if(parentReactor instanceof EmbeddedScriptReactor || parentReactor instanceof EmbeddedRoutineReactor) {
	    		parentReactor.getCurRow().add(data);
	    	} else {
	    		GenRowStruct parentQSInput = parentReactor.getNounStore().makeNoun(PixelDataType.QUERY_STRUCT.toString());
				parentQSInput.add(data);
	    	}
		}
	}
	
	private AbstractQueryStruct createQueryStructPlan() {
		organizeKeys();
		String query = this.keyValue.get(this.keysToGet[0]);
		String connectionUrl = this.keyValue.get(this.keysToGet[1]);
		String driver = this.keyValue.get(this.keysToGet[2]);
		String userName = this.keyValue.get(this.keysToGet[3]);
		String password = this.keyValue.get(this.keysToGet[4]);

		// need to maintain what the FE passed to create this 
		Map<String, Object> config = new HashMap<String, Object>();
		config.put(this.keysToGet[0], connectionUrl);
		config.put(this.keysToGet[1], driver);
		config.put(this.keysToGet[2], userName);
		config.put(this.keysToGet[3], password);

		if(driver == null) {
			throw new IllegalArgumentException("Must pass in the rdbms type");
		}
		RdbmsTypeEnum dbType = RdbmsTypeEnum.getEnumFromString(driver);
		if(dbType == null) {
			// try one more time
			dbType =  RdbmsTypeEnum.getEnumFromDriver(driver);
			if(dbType == null) {
				throw new IllegalArgumentException("Unable to find driver for rdbms type = " + driver);
			}
		}
		
		TemporalEngineHardQueryStruct qs = new TemporalEngineHardQueryStruct();
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY);
		qs.setConfig(config);
		qs.setEngineId("FAKE_ENGINE");
		if(query != null && !query.isEmpty()) {
			query = Utility.decodeURIComponent(query);
			qs.setQuery(query);
		}
		this.qs = qs;
		return this.qs;
	}

}
