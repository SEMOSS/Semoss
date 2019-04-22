package prerna.query.querystruct;

import java.util.Map;

public class HardSelectQueryStruct extends SelectQueryStruct {

	private String hardQuery;
	private Map<String, Object> config = null;
	
	public HardSelectQueryStruct() {
		
	}
	
	public void setQuery(String query) {
		this.hardQuery = query;
	}
	
	public String getQuery() {
		return this.hardQuery;
	}
	
	public void setConfig(Map<String, Object> config) {
		this.config = config;
	}
	
	public Map<String, Object> getConfig() {
		return this.config;
	}
	
	@Override
	public SelectQueryStruct getNewBaseQueryStruct() {
		HardSelectQueryStruct newQs = new HardSelectQueryStruct();
		newQs.setQsType(getQsType());
		newQs.setEngineId(getEngineId());
		// set the physical engine object if appropriate
		newQs.setEngine(getEngine());
		newQs.setConfig(this.config);
		return newQs;
	}

}
