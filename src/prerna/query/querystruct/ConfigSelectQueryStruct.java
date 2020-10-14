package prerna.query.querystruct;

import java.util.Map;

public class ConfigSelectQueryStruct extends SelectQueryStruct {
	
	private Map<String, Object> config = null;

	public Map<String, Object> getConfig() {
		return config;
	}

	public void setConfig(Map<String, Object> config) {
		this.config = config;
	}
	
}
