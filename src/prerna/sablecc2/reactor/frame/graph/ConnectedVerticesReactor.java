package prerna.sablecc2.reactor.frame.graph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.ds.TinkerFrame;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class ConnectedVerticesReactor  extends AbstractFrameReactor {

	private static final String DEGREE_SEPERATION = "deg"; 
	private static final String DIRECTION = "dir"; 

	public ConnectedVerticesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.COLUMN.getKey(), 
				ReactorKeysEnum.VALUES.getKey(), DEGREE_SEPERATION, DIRECTION};
	}

	@Override
	public NounMetadata execute() {
		TinkerFrame tinker = (TinkerFrame) getFrame();
		tinker.getFrameFilters().removeAllFilters();
		
		String nodeType = getColumn();
		List<String> nodeValues = getValues();
		int seperation = getDegreeSep();
		String direction = getDirection();
		findConnectedVertices(tinker, nodeType, nodeValues, seperation, direction);
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.FRAME_FILTER);
	}

	public static void findConnectedVertices(TinkerFrame tf, String columnType, List<String> instances, int numTraversals, String direction) {
		// get the correct physical name
		String nodeType = tf.getMetaData().getPhysicalName(columnType);
		
		//keep set of all vertices to keep
		List<Vertex> instancesToKeep = new Vector<Vertex>();
		
		GraphTraversal<Vertex, Vertex> t1 = tf.g.traversal().V()
				.has(TinkerFrame.TINKER_TYPE, nodeType)
				.has(TinkerFrame.TINKER_NAME, P.within(instances))
				.emit();
		
		// use if we want directions or both
		if(direction.equals("in")) {
			t1 = t1.repeat(__.in().simplePath());
		} else if(direction.equals("out")) {
			t1 = t1.repeat(__.out().simplePath());
		} else {
			t1 = t1.repeat(__.both().simplePath());
		}
		
		t1 = t1.times(numTraversals).dedup();
		
		while(t1.hasNext()) {
			Vertex v = t1.next();
			instancesToKeep.add(v);
		}
		
		int size = instancesToKeep.size();
		if(size == 0) {
			throw new IllegalStateException("Could not find any paths");
		}
		
		Map<String, List<String>> colToValues = new HashMap<String, List<String>>();
		for(Vertex v : instancesToKeep) {
			String type = v.value(TinkerFrame.TINKER_TYPE);
			String value = v.value(TinkerFrame.TINKER_NAME);
			
			List<String> values = null;
			if(colToValues.containsKey(type)) {
				values = colToValues.get(type);
			} else {
				values = new Vector<String>();
				colToValues.put(type, values);
			}
			
			values.add(value);
		}
		
		// need to add the filters to the graph
		for(String type : colToValues.keySet()) {
			NounMetadata lComparison = new NounMetadata(new QueryColumnSelector(type), PixelDataType.COLUMN);
			NounMetadata rComparison = new NounMetadata(colToValues.get(type), PixelDataType.CONST_STRING);
			IQueryFilter newFilter = new SimpleQueryFilter(lComparison, "==", rComparison);
			tf.getFrameFilters().addFilters(newFilter);
		}
	}


	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	private String getColumn() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null) {
			return (String) grs.get(0);
		}

		List<String> vals = this.curRow.getAllStrValues();
		if(!vals.isEmpty()) {
			return vals.get(0);
		}

		throw new IllegalArgumentException("Must define the node type");
	}

	private List<String> getValues() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[2]);
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}

		List<String> vals = this.curRow.getAllStrValues();
		if(vals.size() > 3) {
			// index 0 is the column
			vals.remove(0);
			return vals;
		}

		throw new IllegalArgumentException("Must define at least 2 nodes to find connections between");
	}
	
	private int getDegreeSep() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[3]);
		if(grs != null && !grs.isEmpty()) {
			return ((Number) grs.get(0)).intValue();
		}

		List<Object> vals = this.curRow.getAllNumericColumns();
		if(!vals.isEmpty()) {
			return ((Number) vals).intValue();
		}
		
		throw new IllegalArgumentException("Must define a value for the degrees of seperation");
	}
	
	private String getDirection() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[4]);
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0).toString().toLowerCase();
		}

		return "both";
	}
}
