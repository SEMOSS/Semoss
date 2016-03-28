package prerna.ds;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.rdf.query.builder.GremlinBuilder;
import prerna.util.Constants;

public class TinkerFrameIterator implements Iterator<Object[]> {

	private String dataType; //the data property to be used from the table
	private GraphTraversal gt; //the traversal on the graph
	private List<String> selectors; //the selectors on the table
	
	public TinkerFrameIterator(Graph g, Map<String, Object> options, boolean getRawData) {
		dataType = getRawData ? Constants.VALUE : Constants.NAME;
		
		GremlinBuilder.DIRECTION dir = null;
		if(options.containsKey(TinkerFrame.SORT_BY_DIRECTION)) {
			String strDir = (String) options.get(TinkerFrame.SORT_BY_DIRECTION);
			if(strDir == "desc") {
				dir = GremlinBuilder.DIRECTION.DECR;
			} else {
				dir = GremlinBuilder.DIRECTION.INCR;
			}
		}
//		Integer limit = (Integer) options.get(TinkerFrame.LIMIT);
//		if(limit == null) {
//			limit = -1;
//		}
//		Integer offset = (Integer) options.get(TinkerFrame.OFFSET);
//		if(offset == null) {
//			offset = -1;
//		}
		
		Boolean dedup = false;
		if(options.containsKey(TinkerFrame.DE_DUP)) {
			dedup = (Boolean) options.get(TinkerFrame.DE_DUP);
		}
		
		this.gt = openTraversal(
				(List<String>) options.get(TinkerFrame.SELECTORS), 
				g, 
				(Integer) options.get(TinkerFrame.LIMIT), 
				(Integer) options.get(TinkerFrame.OFFSET), 
				(String) options.get(TinkerFrame.SORT_BY),
				dir, 
				dedup);
	}
	
	private GraphTraversal openTraversal(List<String> selectors, Graph g, Integer start, Integer end, String sortColumn, GremlinBuilder.DIRECTION orderByDirection, Boolean dedup) {
		this.selectors = selectors;
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(selectors, g);
		builder.orderBySelector = sortColumn;
		builder.orderByDirection = orderByDirection;
		//finally execute it to get the executor
		if(start != null && start != -1) {
			builder.setRange(start, end);
		}
		
		GraphTraversal gt = null;
		if(dedup) {
			gt = builder.executeScript().dedup();
		} else {
			gt = (GraphTraversal <Vertex, Map<String, Object>>) builder.executeScript();
		}
		return gt;
	}
	
	@Override
	public boolean hasNext() {
		return gt.hasNext();
	}
	
	@Override
	public Object[] next() {
		
		Object data = gt.next();
		Object [] retObject = new Object[selectors.size()];

		//data will be a map for multi columns
		if(data instanceof Map) {
			for(int colIndex = 0;colIndex < selectors.size();colIndex++) {
				Map<String, Object> mapData = (Map<String, Object>)data; //cast to map
				retObject[colIndex] = ((Vertex)mapData.get(selectors.get(colIndex))).property(dataType).value();
			}
		} else {
			retObject[0] = ((Vertex)data).property(dataType).value();
		}

		return retObject;
	}
}
