package prerna.ds.gexf;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractGexfIterator implements IGexfIterator {

	// string containing the nodes and its properties
	protected String nodeMap;
	protected String[] nodeMapSplit;
	protected int nodeIndex = 0;
	
	// string containing the edges and their properties
	protected String edgeMap;
	protected String[] edgeMapSplit;
	protected int edgeIndex = 0;
	
	// map containing the alias to apply to nodes
	protected Map<String, String> aliasMap;
	
	/**
	 * Default constructor for the gexf iterator
	 * @param selectorMap
	 * @param edgeMap
	 * @param aliasMap
	 */
	public AbstractGexfIterator(String nodeMap, String edgeMap, Map<String, String> aliasMap) {
		this.nodeMap = nodeMap;
		this.edgeMap = edgeMap;
		this.aliasMap = aliasMap;
		
		if(this.nodeMap == null) {
			this.nodeMap = "";
		}
		if(this.edgeMap == null) {
			this.edgeMap = "";
		}
		if(this.aliasMap == null) {
			this.aliasMap = new HashMap<String, String>();
		}
		
		this.nodeMapSplit = this.nodeMap.split(";");
		this.edgeMapSplit = this.edgeMap.split(";");
	}
	
	@Override
	public String getStartString() {
		return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
				+ "<gexf xmlns=\"http://www.gexf.net/1.2draft\" xmlns:viz=\"http://www.gexf.net/1.1draft/viz\" "
				+ "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
				+ "xsi:schemaLocation=\"http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd\" "
				+ "version=\"1.2\">"
				+ "<graph mode=\"static\" defaultedgetype=\"directed\">";
	}
	
	@Override
	public String getEndString() {
		return "</graph></gexf>";
	}
	
	@Override
	public String getNodeStart() {
		return "<nodes>";
	}
	
	@Override
	public String getNodeEnd() {
		return "</nodes>";
	}
	
	@Override
	public String getEdgeStart() {
		return "<edges>";
	}
	
	@Override
	public String getEdgeEnd() {
		return "</edges>";
	}
}
