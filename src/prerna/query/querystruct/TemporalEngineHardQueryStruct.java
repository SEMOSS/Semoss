package prerna.query.querystruct;

import java.util.Map;

public class TemporalEngineHardQueryStruct extends HardSelectQueryStruct {

	private Map<String, Object> config = null;
	
	public TemporalEngineHardQueryStruct() {
		
	}
	
	public void setConfig(Map<String, Object> config) {
		this.config = config;
	}
	
	public Map<String, Object> getConfig() {
		return this.config;
	}
	
	@Override
	public SelectQueryStruct getNewBaseQueryStruct() {
		TemporalEngineHardQueryStruct newQs = new TemporalEngineHardQueryStruct();
		newQs.setQsType(getQsType());
		newQs.setEngineId(getEngineId());
		// set the physical engine object if appropriate
		newQs.setEngine(getEngine());
		newQs.setConfig(this.config);
		return newQs;
	}

}
