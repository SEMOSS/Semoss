package prerna.reactor.qs.source;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.TemporalEngineHardQueryStruct;
import prerna.reactor.EmbeddedRoutineReactor;
import prerna.reactor.EmbeddedScriptReactor;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class JdbcSourceReactor extends AbstractQueryStructReactor {
	
	public JdbcSourceReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.CONNECTION_DETAILS.getKey()};
	}
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		Map<String, Object> connectionDetails = getConDetails();
		
		String driver = (String) connectionDetails.get(AbstractSqlQueryUtil.DRIVER_NAME);
		RdbmsTypeEnum driverEnum = RdbmsTypeEnum.getEnumFromString(driver);
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(driverEnum);
		
		Connection con = null;
		String connectionUrl = null;
		try {
			connectionUrl = queryUtil.setConnectionDetailsfromMap(connectionDetails);
		} catch (RuntimeException e) {
			throw new SemossPixelException(new NounMetadata("Unable to generation connection url with message " + e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		try {
			con = AbstractSqlQueryUtil.makeConnection(queryUtil, connectionUrl, connectionDetails);
			queryUtil.enhanceConnection(con);
		} catch (SQLException e) {
			e.printStackTrace();
			String driverError = e.getMessage();
			String errorMessage = "Unable to establish connection given the connection details.\nDriver produced error: \" ";
			errorMessage += driverError;
			errorMessage += " \"";
			throw new SemossPixelException(new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		RDBMSNativeEngine temporalEngine = null;
//		if(driver.toLowerCase().contains("impala")) {
//			temporalEngine = new ImpalaEngine();
//		} else {
			temporalEngine = new RDBMSNativeEngine();
//		}
		temporalEngine.setEngineId("FAKE_ENGINE");
		temporalEngine.setConnection(con);
		temporalEngine.setBasic(true);
		temporalEngine.setQueryUtil(queryUtil);
		
		TemporalEngineHardQueryStruct qs = new TemporalEngineHardQueryStruct();
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY);
		qs.setConfig(connectionDetails);
		qs.setEngine(temporalEngine);
		this.qs = qs;
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
		Map<String, Object> connectionDetails = getConDetails();
		
		TemporalEngineHardQueryStruct qs = new TemporalEngineHardQueryStruct();
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY);
		qs.setConfig(connectionDetails);
		qs.setEngineId("FAKE_ENGINE");
		this.qs = qs;
		return this.qs;
	}
	
	private Map<String, Object> getConDetails() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.CONNECTION_DETAILS.getKey());
		if(grs != null && !grs.isEmpty()) {
			List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
			if(mapInput != null && !mapInput.isEmpty()) {
				return (Map<String, Object>) mapInput.get(0);
			}
		}
		
		List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
		if(mapInput != null && !mapInput.isEmpty()) {
			return (Map<String, Object>) mapInput.get(0);
		}
		
		return null;
	}
	
}
