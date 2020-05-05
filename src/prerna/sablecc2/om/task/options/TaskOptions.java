package prerna.sablecc2.om.task.options;

import java.util.Map;
import java.util.Set;

import prerna.sablecc2.reactor.export.IFormatter;

public class TaskOptions {

	private Map<String, Object> options;
	private boolean ornament = false;
	private transient IFormatter formatter;
	
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
	
	public boolean isOrnament() {
		return this.ornament;
	}
	
	public void setOrnament(boolean ornament) {
		this.ornament = ornament;
	}

	public Map<String, Object> getAlignmentMap(String panelId) {
		Object pOptions = this.options.get(panelId);
		if(pOptions instanceof Map) {
			Map<String, Object> panelOptions = (Map<String, Object>) pOptions;
			if(panelOptions != null) {
				if(panelOptions.containsKey("alignment")) {
					return (Map<String, Object>) panelOptions.get("alignment");
				}
			}
		}
		return null;
	}

	public String getLayout(String panelId) {
		// this is what I need to change for layout
		Object pOptions = this.options.get(panelId);
		if(pOptions instanceof Map) {
			Map<String, Object> panelOptions = (Map<String, Object>) pOptions;
			if(panelOptions != null) {
				if(panelOptions.containsKey("layout")) {
					return (String) panelOptions.get("layout");
				}
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
	
	public void clear() {
		this.options.clear();
	}
	
	public void setFormatter(IFormatter formatter) {
		this.formatter = formatter;
	}
	
	public IFormatter getFormatter() {
		return this.formatter;
	}
}
