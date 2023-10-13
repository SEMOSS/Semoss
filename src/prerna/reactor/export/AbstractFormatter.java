package prerna.reactor.export;

import java.util.Map;

public abstract class AbstractFormatter implements IFormatter {

	protected Map<String, Object> optionsMap;
	
	@Override
	public void setOptionsMap(Map<String, Object> optionsMap) {
		this.optionsMap = optionsMap;
	}
	
	@Override
	public Map<String, Object> getOptionsMap() {
		return this.optionsMap;
	}
}
