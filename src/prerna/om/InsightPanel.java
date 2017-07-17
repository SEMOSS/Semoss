package prerna.om;

import java.util.HashMap;
import java.util.Map;

public class InsightPanel {

	private String panelId;
	private String panelLabel;
	private transient Map<String, Object> ornaments;
	
	public InsightPanel(String panelId) {
		this.panelId = panelId;
		this.panelLabel = panelId;
		this.ornaments = new HashMap<String, Object>();
	}
	
	public Map<String, Object> getOrnaments() {
		return this.ornaments;
	}
	
	/**
	 * traversalPath is a period delimited key traversal to go through the ornaments object
	 * @param traversalPath
	 * @return
	 */
	public Object getOrnament(String traversalPath) {
		String[] traversal = traversalPath.split("\\.");

		// check first key
		if(!this.ornaments.containsKey(traversal[0])) {
			return new HashMap<String, Object>();
		}
		Object innerObj = this.ornaments.get(traversal[0]);
		
		// go to the necessary depth required
		int depth = traversal.length;
		for(int i = 1; i < depth; i++) {
			if(innerObj instanceof Map) {
				Map innerMap = (Map) innerObj;
				if(innerMap.containsKey(traversal[i])) {
					innerObj = innerMap.get(traversal[i]);
				} else {
					// well, can't find this...
					// just return empty
					return new HashMap<String, Object>();
				}
			} else {
				// well, can't find this...
				// just return empty
				return new HashMap<String, Object>();
			}
		}
		
		return innerObj;
	}
	
	/**
	 * Merge new ornaments into the existing ornament map
	 * @param ornaments
	 */
	public void setOrnaments(Map<String, Object> ornaments) {
		this.ornaments.putAll(ornaments);
	}
	
	/**
	 * Remove all ornaments
	 */
	public void resetOrnaments() {
		this.ornaments.clear();
	}
	
	public String getPanelId() {
		return this.panelId;
	}
	
	public void setPanelId(String panelId) {
		this.panelId = panelId;
	}
	
	public void setPanelLabel(String panelLabel) {
		this.panelLabel = panelLabel;
	}
	
	public String getPanelLabel() {
		return this.panelLabel;
	}
	
	/**
	 * Take all the properties of another insight panel
	 * and set them for this panel
	 * @param existingPanel
	 */
	public void clone(InsightPanel existingPanel) {
		//TODO: make sure I update this every time i add a new field!!!
		this.ornaments.putAll(existingPanel.ornaments);
	}
	
}
