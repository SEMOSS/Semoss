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

//	private boolean useRawData;
	private String dataType;
	private GraphTraversal gt;
	String[] headerNames;
	Vector<String> finalColumns;
		
	public TinkerFrameIterator(String[] headers, List<String> columnsToSkip, Graph g) {
		this.headerNames = headers;
		this.gt = openTraversal(columnsToSkip, g);
		dataType = Constants.VALUE;
	}
	
	//creating a separate constructor to take in raw data boolean because to ease the transition out of using getRawData 
	public TinkerFrameIterator(String[] headers, List<String> columnsToSkip, Graph g, boolean useRawData) {
		this.headerNames = headers;
		this.gt = openTraversal(columnsToSkip, g);
		dataType = useRawData ? Constants.VALUE : Constants.NAME;
	}
	
	private GraphTraversal openTraversal(List<String> columnsToSkip, Graph g) {
		Vector <String> finalColumns = new Vector<String>();
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(headerNames, columnsToSkip, g);

		// add everything that you need
		for(int colIndex = 0;colIndex < headerNames.length;colIndex++) // add everything you want first
		{
			if(!columnsToSkip.contains(headerNames[colIndex])) {
				finalColumns.add(headerNames[colIndex]);
			}
		}

		// now add the projections
		this.finalColumns = finalColumns;
		
		//finally execute it to get the executor
		GraphTraversal <Vertex, Map<String, Object>> gt = (GraphTraversal <Vertex, Map<String, Object>>)builder.executeScript(g);
		return gt;
	}
	
	@Override
	public boolean hasNext() {
		return gt.hasNext();
	}
	
	@Override
	public Object[] next() {
		
		Object data = gt.next();
		Object [] retObject = new Object[finalColumns.size()];

		//data will be a map for multi columns
		if(data instanceof Map) {
			for(int colIndex = 0;colIndex < finalColumns.size();colIndex++) {
				Map<String, Object> mapData = (Map<String, Object>)data; //cast to map
				retObject[colIndex] = ((Vertex)mapData.get(finalColumns.get(colIndex))).property(dataType).value();
				
//				Object o = mapData.get(finalColumns.get(colIndex));
////				if(o instanceof List) {
////					List l = (List)o;
////					retObject[colIndex] = ((Vertex)l.get(0)).property(dataType).value();
////				} else {
//					retObject[colIndex] = ((Vertex)mapData.get(finalColumns.get(colIndex))).property(dataType).value();
////				}
			}
		} else {
			retObject[0] = ((Vertex)data).property(dataType).value();
		}

		return retObject;
	}
}
