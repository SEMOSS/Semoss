package prerna.ds;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TinkerIterator implements Iterator {
	private GraphTraversal gt;
	private List<String> selectors;
	private HashMap<String, String> propHash;
	private Vector<String> nodeSelector;
	private Hashtable<String, Hashtable<String, Vector>> filters;

	public TinkerIterator(GraphTraversal gt, List<String> selectors, HashMap<String, String> propHash,
			Vector<String> nodeSelector, Hashtable<String, Hashtable<String, Vector>> filters) {
		this.gt = gt;
		this.selectors = selectors;
		this.propHash = propHash;
		this.nodeSelector = nodeSelector;
		this.filters = filters;

		for (String sel : selectors) {
			if (filters.containsKey(sel)) {
				if (propHash.containsKey(sel)) {
					gt = addFilterInPath(this.gt, sel, filters.get(sel));
				}
			}
		}
	}

	@Override
	public boolean hasNext() {
		return gt.hasNext();
	}

	@Override
	public Object[] next() {

		Object data = gt.next();
		Object[] retObject = new Object[selectors.size()];

		// data will be a map for multi columns
		if (data instanceof Map) {
			for (int colIndex = 0; colIndex < selectors.size(); colIndex++) {
				Map<String, Object> mapData = (Map<String, Object>) data;
				String select = selectors.get(colIndex);

				// retrieve property
				if (propHash.containsKey(select)) {
					System.out.println("************* NODE: " + propHash.get(select));
					System.out.println("************* Property: " + select);
					Vertex vert = (Vertex) mapData.get(propHash.get(select));
					Set<String> vertProps = vert.keys();
					if (vertProps.contains(select)) {
						retObject[colIndex] = vert.property(select).value();

					} else {
						retObject[colIndex] = null;
					}
				}
				// retrieve node value
				else {
					retObject[colIndex] = ((Vertex) mapData.get(select)).property(TinkerFrame.TINKER_NAME).value();
				}
				// retObject[colIndex] = ((Vertex)
				// mapData.get(selectors.get(colIndex))).property(TinkerFrame.TINKER_NAME)
				// .value();
			}
		} else {

			for (int colIndex = 0; colIndex < selectors.size(); colIndex++) {
				Vertex vert = (Vertex) data;
				String select = selectors.get(colIndex);
				// retrieve property
				if (propHash.containsKey(select)) {
					System.out.println("************* NODE: " + propHash.get(select));
					System.out.println("************* Property: " + select);
					Set<String> vertProps = vert.keys();
					if (vertProps.contains(select)) {
						retObject[colIndex] = vert.property(select).value();
					} else {
						retObject[colIndex] = null;
					}
				}
				// retrieve node value
				else {
					retObject[colIndex] = vert.property(TinkerFrame.TINKER_NAME).value();
				}
			}
		}

		return retObject;
	}

	public GraphTraversal<Object,Vertex> addFilterInPath(GraphTraversal<Object, Vertex> gt, String nameType,
			Hashtable<String, Vector> filterInfo) {
		// TODO: right now, if its a math, assumption that vector only contains
		// one value
		for (String filterType : filterInfo.keySet()) {
			Vector filterVals = filterInfo.get(filterType);
			if (filterType.equals("=")) {
				// if(filterVals.get(0) instanceof Number) {
				// gt = gt.has(TinkerFrame.TINKER_NAME, P.eq(filterVals.get(0)
				// ));
				// } else {
				gt = gt.has(propHash.get(nameType)).has(nameType, P.within(filterVals.toArray()));
				// }
			} else if (filterType.equals("<")) {
				gt = gt.has(propHash.get(nameType)).has(nameType, P.lt(filterVals.get(0)));
			} else if (filterType.equals(">")) {
				gt = gt.has(propHash.get(nameType)).has(nameType, P.gt(filterVals.get(0)));
			} else if (filterType.equals("<=")) {
				gt = gt.has(propHash.get(nameType)).has(nameType, P.lte(filterVals.get(0)));
			} else if (filterType.equals(">=")) {
				gt = gt.has(propHash.get(nameType)).has(nameType, P.gte(filterVals.get(0)));
			} else if (filterType.equals("!=")) {
				// if(filterVals.get(0) instanceof Number) {
				// gt = gt.has(TinkerFrame.TINKER_NAME, P.neq(filterVals.get(0)
				// ));
				// } else {
				gt = gt.has(propHash.get(nameType)).has(nameType, P.without(filterVals.toArray()));
				// }
			}
		}
		return gt;
	}
}
