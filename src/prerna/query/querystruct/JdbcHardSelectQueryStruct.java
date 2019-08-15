package prerna.query.querystruct;

import java.util.Map;

public class JdbcHardSelectQueryStruct extends HardSelectQueryStruct {

	private Map<String, Object> config = null;
	
	public JdbcHardSelectQueryStruct() {
		
	}
	
	public void setConfig(Map<String, Object> config) {
		this.config = config;
	}
	
	public Map<String, Object> getConfig() {
		return this.config;
	}
	
	@Override
	public QUERY_STRUCT_TYPE getQsType() {
		return QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY;
	}
	
	@Override
	public SelectQueryStruct getNewBaseQueryStruct() {
		JdbcHardSelectQueryStruct newQs = new JdbcHardSelectQueryStruct();
		newQs.setQsType(getQsType());
		newQs.setEngineId(getEngineId());
		// set the physical engine object if appropriate
		newQs.setEngine(getEngine());
		newQs.setConfig(this.config);
		return newQs;
	}

}
