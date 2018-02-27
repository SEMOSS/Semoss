package prerna.sablecc2.om.task.options;

import java.util.List;
import java.util.Map;
import java.util.Vector;

public class TaskOptions {
	
	private List<Map<String, Object>> options;
	
	/**
	 * Constructor if we have the list
	 * @param options
	 */
	public TaskOptions(List<Map<String, Object>> options) {
		this.options = options; 
	}
	
	
	/**
	 * Constructor for single element to append to list
	 * @param options
	 */
	public TaskOptions(Map<String, Object> options) {
		this.options = new Vector<Map<String, Object>>();
		this.options.add(options);
	}

	public List<String> getPanelIds() {
		Vector<String> panelIds = new Vector<String>();
		for(int i = 0; i < this.options.size(); i++) {
			Map<String, Object> map = this.options.get(i);
			for(String key: map.keySet()) {
				panelIds.add(key);
			}
		}
		return panelIds;
	}
	
	public Map<String, Object> getAlignmentMap(String panelId) {
		for(int i = 0; i < this.options.size(); i++) {
			Map<String, Object> panelIdMap = this.options.get(i);
			Map<String, Object> panelOptions = (Map<String, Object>) panelIdMap.get(panelId);
			if(panelOptions != null) {
				if(panelOptions.containsKey("alignment")) {
					return (Map<String, Object>) panelOptions.get("alignment");
				}
			}
		}
		return null;
	}
	
	public String getLayout(String panelId) {
		for(int i = 0; i < this.options.size(); i++) {
			Map<String, Object> panelIdMap = this.options.get(i);
			Map<String, Object> panelOptions = (Map<String, Object>) panelIdMap.get(panelId);
			if(panelOptions != null) {
				if(panelOptions.containsKey("layout")) {
					return (String) panelOptions.get("layout");
				}
			}
		}
		return null;
	}
	
	public List<Map<String, Object>> getOptions() {
		return this.options;
	}

	public boolean isEmpty() {
		return this.options.isEmpty();
	}
}
