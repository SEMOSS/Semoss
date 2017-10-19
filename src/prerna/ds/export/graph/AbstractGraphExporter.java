package prerna.ds.export.graph;

import java.util.Hashtable;
import java.util.Map;

public abstract class AbstractGraphExporter implements IGraphExporter {
	
	// store the count of each node type
	protected Map<String, Integer> vertCounts = new Hashtable<String, Integer>();
	
	protected void addVertCount(String type) {
		if(vertCounts.containsKey(type)) {
			vertCounts.put(type, vertCounts.get(type) + 1);
		} else {
			vertCounts.put(type,1);
		}
	}
	
	public Map<String, Integer> getVertCounts() {
		return this.vertCounts;
	}
	
}
