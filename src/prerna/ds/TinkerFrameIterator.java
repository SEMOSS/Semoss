package prerna.ds;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.rdf.query.builder.GremlinBuilder;
import prerna.util.Utility;

public class TinkerFrameIterator implements Iterator<Object[]> {

//	private String dataType; //the data property to be used from the table
	private GraphTraversal gt; //the traversal on the graph
	private List<String> selectors; //the selectors on the table
	
	public TinkerFrameIterator(Graph g, Graph metaG, Map<String, Object> options) { //, boolean getRawData) {
//		dataType = getRawData ? TinkerFrame.TINKER_VALUE : TinkerFrame.TINKER_NAME;
		
		GremlinBuilder.DIRECTION dir = null;
		if(options.containsKey(AbstractTableDataFrame.SORT_BY_DIRECTION)) {
			String strDir = (String) options.get(AbstractTableDataFrame.SORT_BY_DIRECTION);
			if(strDir == "desc") {
				dir = GremlinBuilder.DIRECTION.DECR;
			} else {
				dir = GremlinBuilder.DIRECTION.INCR;
			}
		}
		
		Boolean dedup = false;
		if(options.containsKey(AbstractTableDataFrame.DE_DUP)) {
			dedup = (Boolean) options.get(AbstractTableDataFrame.DE_DUP);
		}
		
		Map<Object, Object> temporalBindings = (Map<Object, Object>) options.get(AbstractTableDataFrame.TEMPORAL_BINDINGS); 
		// clean values always put into list so bifurcation in logic doesn't need to exist elsewhere
		Map<String, List<Object>> cleanTemporalBindings = new Hashtable<String, List<Object>>();
		if(temporalBindings != null) {
			for(Object key : temporalBindings.keySet()) {
				Object val = temporalBindings.get(key);
				List<Object> cleanVal = new Vector<Object>();
				// if passed back a list
				if(val instanceof Collection) {
					Collection<? extends Object> collectionVal = (Collection<? extends Object>) val;
					for(Object valObj : collectionVal) {
						Object cleanObj = null;
						String strObj = valObj.toString().trim();
						String type = Utility.findTypes(strObj)[0] + "";
						if(type.equalsIgnoreCase("Date")) {
							cleanObj = Utility.getDate(strObj);
						} else if(type.equalsIgnoreCase("Double")) {
							cleanObj = Utility.getDouble(strObj);
						} else {
							cleanObj = Utility.cleanString(strObj, true, true, false);
						}
						((Vector) cleanVal).add(cleanObj);
					}
					// only need to clean the instances
					cleanTemporalBindings.put(key + "", cleanVal);
				} else {
					// this means it is a single value
					Object cleanObj = null;
					String strObj = val.toString().trim();
					String type = Utility.findTypes(strObj)[0] + "";
					if(type.equalsIgnoreCase("Date")) {
						cleanObj = Utility.getDate(strObj);
					} else if(type.equalsIgnoreCase("Double")) {
						cleanObj = Utility.getDouble(strObj);
					} else {
						cleanObj = Utility.cleanString(strObj, true, true, false);
					}
					cleanVal.add(cleanObj);
					// only need to clean the instances
					cleanTemporalBindings.put(key + "", cleanVal);
				}
			}
		}
		
		this.gt = openTraversal(
				(List<String>) options.get(AbstractTableDataFrame.SELECTORS), 
				g, metaG,
				(Integer) options.get(AbstractTableDataFrame.OFFSET), 
				(Integer) options.get(AbstractTableDataFrame.LIMIT), 
				(String) options.get(AbstractTableDataFrame.SORT_BY),
				cleanTemporalBindings,
				dir, 
				dedup);
	}

	private GraphTraversal openTraversal(List<String> selectors, Graph g, Graph metaG, Integer offset, Integer limit,
			String sortColumn, Map<String, List<Object>> cleanTemporalBindings,
			GremlinBuilder.DIRECTION orderByDirection, Boolean dedup) {
		this.selectors = selectors;
		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(selectors, g, metaG, cleanTemporalBindings);
		builder.setOrderBySelector(sortColumn);
		builder.setOrderByDirection(orderByDirection);
		builder.setDedup(dedup);
		if(offset != null && offset > -1) {
			builder.setRange(offset, limit + offset);
		} else if(limit != null){
			builder.setRange(0, limit);
		}
		GraphTraversal gt = (GraphTraversal <Vertex, Map<String, Object>>) builder.executeScript();
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
				retObject[colIndex] = ((Vertex)mapData.get(selectors.get(colIndex))).property(TinkerFrame.TINKER_NAME).value();
			}
		} else {
			retObject[0] = ((Vertex)data).property(TinkerFrame.TINKER_NAME).value();
		}

		return retObject;
	}
}
