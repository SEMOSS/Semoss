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
	List<String> selectors; //the selectors on the table
	
	public TinkerFrameIterator(List<String> selectors, Graph g) {
		this(selectors, g, false);
	}
	
	public TinkerFrameIterator(List<String> selectors, Graph g, boolean useRawData) {
		this.selectors = selectors;
		dataType = useRawData ? Constants.VALUE : Constants.NAME;
		this.gt = openTraversal(selectors, g, -1, -1, null, null);
	}
	
	public TinkerFrameIterator(List<String> selectors, Graph g, boolean useRawData, int start, int end) {
		this.selectors = selectors;
		dataType = useRawData ? Constants.VALUE : Constants.NAME;
		this.gt = openTraversal(selectors, g, start, end, null, null);
	}
	
	public TinkerFrameIterator(List<String> selectors, Graph g, boolean useRawData, String sortColumn, GremlinBuilder.DIRECTION orderByDirection) {
		this.selectors = selectors;
		dataType = useRawData ? Constants.VALUE : Constants.NAME;
		this.gt = openTraversal(selectors, g, -1, -1, sortColumn, orderByDirection);
	}
	
	public TinkerFrameIterator(List<String> selectors, Graph g, boolean useRawData, int start, int end, String sortColumn, GremlinBuilder.DIRECTION orderByDirection) {
		this.selectors = selectors;
		dataType = useRawData ? Constants.VALUE : Constants.NAME;
		this.gt = openTraversal(selectors, g, start, end, sortColumn, orderByDirection);
	}
	
	private GraphTraversal openTraversal(List<String> selectors, Graph g, int start, int end, String sortColumn, GremlinBuilder.DIRECTION orderByDirection) {
		this.selectors = selectors;
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(selectors, g);
		builder.orderBySelector = sortColumn;
		builder.orderByDirection = orderByDirection;
		//finally execute it to get the executor
		if(start != -1) {
			builder.setRange(start, end);
		}
		GraphTraversal <Vertex, Map<String, Object>> gt = (GraphTraversal <Vertex, Map<String, Object>>)builder.executeScript();
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
