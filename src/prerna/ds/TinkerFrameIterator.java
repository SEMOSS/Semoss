package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.rdf.query.builder.GremlinBuilder;
import prerna.util.Constants;

public class TinkerFrameIterator implements Iterator<Object[]> {

	private boolean useRawData;
	private GraphTraversal gt;
	String[] headerNames;
	
	/**
	 * Constructor for the BTreeIterator
	 * Uses the leaves in the tree to traverse up and get the data corresponding to a row if the tree was flattened
	 * @param typeRoot			A list of nodes corresponding to the leaves in the tree
	 */
//	public TinkerFrameIterator(TreeNode typeRoot) {
//		this(typeRoot, false, null);
//	}
//	
//	public TinkerFrameIterator(TreeNode typeRoot, boolean getRawData) {
//		this(typeRoot, getRawData, null);
//	}
//	
//	public TinkerFrameIterator(TreeNode typeRoot, boolean getRawData, List<String> columns2skip) {
//		iterator = new ValueTreeColumnIterator(typeRoot);
//		useRawData = getRawData;
//		this.columns2skip = columns2skip == null ? new HashSet<String>(0) : new HashSet<String>(columns2skip);
//	}
	
	public TinkerFrameIterator(String[] headers, Map<String, Set<String>> edgeHash, List<String> columnsToSkip, Map<String, List<Object>> filterHash, Graph g) {
		this.headerNames = headers;
		this.gt = openTraversal(headers, edgeHash, columnsToSkip, filterHash, g);
	}
	
	private GraphTraversal openTraversal(String[] headers, Map<String, Set<String>> edgeHash, List<String> columnsToSkip, Map<String, List<Object>> filterHash, Graph g){
		Vector <String> finalColumns = new Vector<String>();
		GremlinBuilder builder = new GremlinBuilder(g);

		//add edges if edges exist
		if(headerNames.length > 1) {
			HashMap<String, Set<String>> edgeMapCopy = new HashMap<String, Set<String>>();
			edgeMapCopy.putAll(edgeHash);
			builder.addNodeEdge(edgeMapCopy);
		} else {
			//no edges exist, add single node to builder
			builder.addNode(headerNames[0]);
		}

		// add everything that you need
		for(int colIndex = 0;colIndex < headerNames.length;colIndex++) // add everything you want first
		{
			if(!columnsToSkip.contains(headerNames[colIndex])) {
				finalColumns.add(headerNames[colIndex]);
			}
		}

		// now add the projections
		builder.addSelector(finalColumns);

		// add the filters next
		for(int colIndex = 0;colIndex < headerNames.length;colIndex++)
		{
			if(filterHash.containsKey(headerNames[colIndex]))
				builder.addFilter(headerNames[colIndex], filterHash.get(headerNames[colIndex]));
		}

		//finally execute it to get the executor
		GraphTraversal <Vertex, Map<String, Object>> gt = (GraphTraversal <Vertex, Map<String, Object>>)builder.executeScript(g);
		return gt;
	}
	
	/**
	 * Perform a non-recursive depth-first-search (DFS)
	 * Must also take into consideration the number of instances associated with each node
	 * Must also take into consideration the fan-out of the btree for siblings of node
	 */
	@Override
	public boolean hasNext() {
		return gt.hasNext();
	}
	
	@Override
	public Object[] next() {
		Object data = gt.next();
		Object [] retObject = new Object[headerNames.length];

		//data will be a map for multi columns
		if(data instanceof Map) {
			for(int colIndex = 0;colIndex < headerNames.length;colIndex++) {
				Map<String, Object> mapData = (Map<String, Object>)data; //cast to map
				retObject[colIndex] = ((Vertex)mapData.get(headerNames[colIndex])).property(Constants.VALUE).value();
			}
		} else {
			retObject[0] = ((Vertex)data).property(Constants.VALUE).value();
		}

		return retObject;
	}
}
