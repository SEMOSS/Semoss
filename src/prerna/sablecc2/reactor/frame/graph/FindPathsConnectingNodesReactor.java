package prerna.sablecc2.reactor.frame.graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
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

public class FindPathsConnectingNodesReactor extends AbstractFrameReactor {

	private static final String DEGREE_SEPERATION = "deg"; 
	private static final String REFRESH_FITLERS = "refresh"; 

	public FindPathsConnectingNodesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.COLUMN.getKey(), 
				ReactorKeysEnum.VALUES.getKey(), DEGREE_SEPERATION, REFRESH_FITLERS};
	}

	@Override
	public NounMetadata execute() {
		TinkerFrame tinker = (TinkerFrame) getFrame();
		
		String nodeType = getColumn();
		List<String> nodeValues = getValues();
		int seperation = getDegreeSep();
		findSharedVertices(tinker, nodeType, nodeValues, seperation);
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.FRAME_FILTER);
	}

	public static void findSharedVertices(TinkerFrame tf, String columnType, List<String> instances, int numTraversals) {
		// get the correct physical name
		String nodeType = tf.getMetaData().getPhysicalName(columnType);
		
		// keep set of all vertices to keep
		Set<Vertex> instancesToKeep = new HashSet<Vertex>();
		
		int numInstances = instances.size();
		for(int index = 0; index < numInstances-1; index++) {
			String instance = instances.get(index);
			// find set of end positions
			List<String> instancesToBind = new Vector<String>(instances.subList(index+1, numInstances));

			GraphTraversal t1 = tf.g.traversal().V().has(TinkerFrame.TINKER_TYPE, nodeType).has(TinkerFrame.TINKER_NAME, instance).as("start");
			for(int i = 0; i < numTraversals; i++) {
				if(i == 0) {
					t1 = t1.both().as( "0deg");
				} else if(i == 1) {
					t1 = t1.both().as( "1deg").where( "1deg", P.neq("start"));
				} else {
					t1 = t1.both().as( i + "deg").where( i + "deg", P.neq( (i-2) + "deg") );
				}
			}
			t1 = t1.has(TinkerFrame.TINKER_NAME, P.within(instancesToBind));
			
			if(numTraversals == 1) {
				t1 = t1.select("start", "0deg");
			} else if(numTraversals >= 2) {
				String[] degreesToSelect = new String[numTraversals - 1];
				for(int degreeCount = 1; degreeCount < numTraversals; degreeCount++) {
					degreesToSelect[degreeCount-1] = degreeCount + "deg";
				}
				t1 = t1.select("start", "0deg", degreesToSelect);
			}

			while(t1.hasNext()) {
//				StringBuilder linkage = new StringBuilder();
				Object data = t1.next();
				if(data instanceof Map) {
					Vertex start = (Vertex) ((Map) data).get("start");
					instancesToKeep.add(start);
//					linkage.append(start.value(TinkerFrame.TINKER_NAME) + "").append(" ->");
					for(int i = 0; i < numTraversals; i++) {
						Vertex v = (Vertex) ((Map) data).get( i + "deg");
						instancesToKeep.add(v);
//						linkage.append(v.value(TinkerFrame.TINKER_NAME) + "").append(" ->");
					}
				} else {
					System.err.println("Ughhh.... shouldn't get here");
				}

//				System.out.println(linkage.toString());
			}
		}

		int size = instancesToKeep.size();
		if(size == 0) {
			throw new IllegalStateException("Cannot find any path given the instances and the degrees of separation");
		}
		
		// remove the current frame filters
		tf.getFrameFilters().removeAllFilters();
		
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
}
