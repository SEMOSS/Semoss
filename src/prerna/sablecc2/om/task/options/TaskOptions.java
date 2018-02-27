package prerna.sablecc2.om.task.options;

import java.util.Map;
import java.util.Set;

public class TaskOptions {

	private Map<String, Object> options;

	/**
	 * Constructor for task options
	 * @param options
	 */
	public TaskOptions(Map<String, Object> options) {
		this.options = options;
	}

	public Set<String> getPanelIds() {
		return this.options.keySet();
	}

	public Map<String, Object> getAlignmentMap(String panelId) {
		Map<String, Object> panelOptions = (Map<String, Object>) this.options.get(panelId);
		if(panelOptions != null) {
			if(panelOptions.containsKey("alignment")) {
				return (Map<String, Object>) panelOptions.get("alignment");
			}
		}
		return null;
	}

	public String getLayout(String panelId) {
		Map<String, Object> panelOptions = (Map<String, Object>) this.options.get(panelId);
		if(panelOptions != null) {
			if(panelOptions.containsKey("layout")) {
				return (String) panelOptions.get("layout");
			}
		}
		return null;
	}

	public Map<String, Object> getOptions() {
		return this.options;
	}

	public boolean isEmpty() {
		return this.options.isEmpty();
	}
}
