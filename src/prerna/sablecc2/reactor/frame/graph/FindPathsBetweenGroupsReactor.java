package prerna.sablecc2.reactor.frame.graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

public class FindPathsBetweenGroupsReactor extends AbstractFrameReactor {

	private static final String COLUMN1 = "column1"; 
	private static final String COLUMN2 = "column2"; 
	private static final String VALUES1 = "values1"; 
	private static final String VALUES2 = "values2"; 
	private static final String MAX_TRAVERSALS = "max"; 

	public FindPathsBetweenGroupsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey(), COLUMN1, COLUMN2,
				VALUES1, VALUES2, MAX_TRAVERSALS};
	}

	@Override
	public NounMetadata execute() {
		TinkerFrame tinker = (TinkerFrame) getFrame();
		tinker.getFrameFilters().removeAllFilters();

		String nodeType1 = getColumn(COLUMN1);
		String nodeType2 = getColumn(COLUMN2);
		List<String> values1 = getValues(VALUES1);
		List<String> values2 = getValues(VALUES2);
		int max = getMaxTraversals();

		findConnectionsBetweenGroups(tinker, nodeType1, values1, nodeType2, values2, max);
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.FRAME_FILTER);
	}

	public static void findConnectionsBetweenGroups(TinkerFrame tf, String type1, List<String> instances1, String type2, List<String> instances2, int numTraversals) {
		// get the correct physical name
		String nodeType1 = tf.getMetaData().getPhysicalName(type1);
		String nodeType2 = tf.getMetaData().getPhysicalName(type2);

		//keep set of all vertices to keep
		Set<Vertex> instancesToKeep = new HashSet<Vertex>();

		GraphTraversal<Vertex, Object> t1 = tf.g.traversal().V()
				.has(TinkerFrame.TINKER_TYPE, nodeType1)
				.has(TinkerFrame.TINKER_NAME, P.within(instances1))
				.emit()
				.repeat(__.both().simplePath())
				.until(
						__.has(TinkerFrame.TINKER_TYPE, nodeType2)
						.has(TinkerFrame.TINKER_NAME, P.within(instances2)).
						or()
						.loops().is(P.eq(numTraversals))
				).has(TinkerFrame.TINKER_NAME, P.within(instances2))
				.path()
				.unfold()
				.dedup();

		while(t1.hasNext()) {
			Vertex v = (Vertex) t1.next();
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

	private String getColumn(String key) {
		GenRowStruct grs = this.store.getNoun(key);
		if(grs != null) {
			return (String) grs.get(0);
		}

		throw new IllegalArgumentException("Must define the input " + key);
	}

	private List<String> getValues(String key) {
		GenRowStruct grs = this.store.getNoun(key);
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}

		throw new IllegalArgumentException("Must define the input "+ key);
	}

	private int getMaxTraversals() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[5]);
		if(grs != null && !grs.isEmpty()) {
			return ((Number) grs.get(0)).intValue();
		}

		List<Object> vals = this.curRow.getAllNumericColumns();
		if(!vals.isEmpty()) {
			return ((Number) vals).intValue();
		}

		throw new IllegalArgumentException("Must define a value for the max number of traversals to attempt");
	}
}
