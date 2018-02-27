package prerna.sablecc2.om.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class TaskOptions {
	List<Object> options;
	public TaskOptions(List<Object> options) {
		this.options = options; 
	}
	
	public List<String> getPanelIds() {
		Vector<String> panelIds = new Vector<String>();
		for(int i = 0; i < this.options.size(); i++) {
			HashMap<String, Object> map = (HashMap<String, Object>) this.options.get(i);
			for(String key: map.keySet()) {
				panelIds.add(key);
			}
		}
		return panelIds;
	}
	
	public Map<String, Object> getAlignmentMap(String panelId) {
		for(int i = 0; i < this.options.size(); i++) {
			HashMap<String, Object> panelIdMap = (HashMap<String, Object>) this.options.get(i);
			HashMap<String, Object> panelOptions = (HashMap<String, Object>) panelIdMap.get(panelId);
			if(panelOptions != null) {
				if(panelOptions.containsKey("alignment")) {
					return (HashMap<String, Object>) panelOptions.get("alignment");
				}
			}
		}
		return null;
	}
	
	public String getLayout(String panelId) {
		for(int i = 0; i < this.options.size(); i++) {
			HashMap<String, Object> panelIdMap = (HashMap<String, Object>) this.options.get(i);
			HashMap<String, Object> panelOptions = (HashMap<String, Object>) panelIdMap.get(panelId);
			if(panelOptions != null) {
				if(panelOptions.containsKey("layout")) {
					return (String) panelOptions.get("layout");
				}
			}
		}
		return null;
	}
}
