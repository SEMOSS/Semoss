package prerna.ds;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TinkerBaseIterator implements Iterator {
	private GraphTraversal gt;
	private List<String> selectors;
	private HashMap<String, String> propHash;
	private Vector<String> nodeSelector;
	private Hashtable<String, Hashtable<String, Vector>> filters;
	private Object[] nextRow;

	public TinkerBaseIterator(GraphTraversal gt, List<String> selectors, HashMap<String, String> propHash,
			Vector<String> nodeSelector, Hashtable<String, Hashtable<String, Vector>> filters) {
		this.gt = gt;
		this.selectors = selectors;
		this.propHash = propHash;
		this.nodeSelector = nodeSelector;
		this.filters = filters;
		boolean validRow = false;
		while (gt.hasNext() && !validRow) {
			nextRow = getNextRow();
			if (nextRow != null) {
				validRow = true;
			}
		}
		if (!validRow) {
			nextRow = null;
		}
	}

	@Override
	public boolean hasNext() {
		return nextRow != null;
	}

	@Override
	public Object[] next() {

		Object[] row = nextRow;
		boolean validRow = false;
		while (gt.hasNext() && !validRow) {
			nextRow = getNextRow();
			if (nextRow != null) {
				validRow = true;
			}
		}
		if (!validRow) {
			nextRow = null;
		}
		return row;

	}

	private Object[] getNextRow() {
		Object data = gt.next();
		Object[] retObject = new Object[selectors.size()];

		// data will be a map for multi columns
		if (data instanceof Map) {
			for (int colIndex = 0; colIndex < selectors.size(); colIndex++) {
				Map<String, Object> mapData = (Map<String, Object>) data;
				String select = selectors.get(colIndex);

				// retrieve property
				if (propHash.containsKey(select)) {
					Vertex vert = (Vertex) mapData.get(propHash.get(select));
					Set<String> vertProps = vert.keys();
					if (vertProps.contains(select)) {
						if (filters.containsKey(select)) {
							Object value = filterProperty(vert.property(select).value(), filters.get(select));
							if (value == null) {
								return null;
							} else {
								retObject[colIndex] = value;
							}
						} else {
							retObject[colIndex] = vert.property(select).value();
						}

					} else {
						// when a value is null
						if (filters.containsKey(select)) {
							// add null if null is in the filterHash
							boolean filterNullEqualTo = false;
							boolean filterNullNotEqualTo = false;
							Hashtable<String, Vector> fil = filters.get(select);
							Vector filterValsEqual = fil.get("=");
							Vector filterValsNotEqual = fil.get("!=");
							// not equal to
							for (Object s : filterValsEqual.toArray()) {
								if (s.equals(AbstractTableDataFrame.VALUE.NULL)) {
									filterNullEqualTo = true;
								}
							}
							for (Object s : filterValsNotEqual.toArray()) {
								if (s.equals(AbstractTableDataFrame.VALUE.NULL)) {
									filterNullNotEqualTo = true;
								}
							}

							// if filter value == null
							if (filterNullEqualTo) {
								retObject[colIndex] = null;
							} else {
								return null;
							}

							if (filterNullNotEqualTo) {
								return null;
							} else {
								retObject[colIndex] = null;
							}
						} else {
							retObject[colIndex] = null;
						}
					}
				}
				// retrieve node value
				else {
					retObject[colIndex] = ((Vertex) mapData.get(select)).property(TinkerFrame.TINKER_NAME).value();
				}

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
						if (filters.containsKey(select)) {
							Object value = filterProperty(vert.property(select).value(), filters.get(select));
							if (value == null) {
								return null;
							} else {
								retObject[colIndex] = value;
							}
						} else {
							retObject[colIndex] = vert.property(select).value();
						}

					} else {
						// null values
						// when a value is null
						if (filters.containsKey(select)) {
							// add null if null is in the filterHash
							boolean filterNullEqualTo = false;
							boolean filterNullNotEqualTo = false;
							Hashtable<String, Vector> fil = filters.get(select);
							Vector filterValsEqual = fil.get("=");
							Vector filterValsNotEqual = fil.get("!=");
							// not equal to
							if (filterValsEqual != null) {
								for (Object s : filterValsEqual.toArray()) {
									if (s.equals(AbstractTableDataFrame.VALUE.NULL)) {
										filterNullEqualTo = true;
									}
								}
							}
							if (filterValsNotEqual != null) {
								for (Object s : filterValsNotEqual.toArray()) {
									if (s.equals(AbstractTableDataFrame.VALUE.NULL)) {
										filterNullNotEqualTo = true;
									}
								}
							}

							// if filter value == null
							if (filterNullEqualTo) {
								retObject[colIndex] = null;
							} else {
								return null;
							}

							if (filterNullNotEqualTo) {
								return null;
							} else {
								retObject[colIndex] = null;
							}
						} else {
							retObject[colIndex] = null;
						}

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

	public Object filterProperty(Object object, Hashtable<String, Vector> filterInfo) {
		Object value = null;
		// TODO: right now, if its a math, assumption that vector only contains
		// one value
		for (String filterType : filterInfo.keySet()) {
			Vector filterVals = filterInfo.get(filterType);
			for (Object s : filterVals.toArray()) {

				if (filterType.equals("=")) {
					if (object instanceof Double) {
						if (s instanceof Integer) {
							int comp = compareDouble(s, object);
							if (comp == 0) {
								value = object;
							}
						}
					}

					if (object instanceof String) {
						if ((((String) s).equals((String) object))) {
							value = object;
						}
					}
				}

				else if (filterType.equals("<")) {
					if (object instanceof Double) {
						if (s instanceof Integer) {
							int comp = compareDouble(s, object);
							if (comp < 0) {
								value = object;
							}
						}
					}
				} else if (filterType.equals(">")) {
					if (object instanceof Double) {
						if (s instanceof Integer) {
							int comp = compareDouble(s, object);
							if (comp > 0) {
								value = object;
							}
						}
					}
				} else if (filterType.equals("<=")) {
					if (object instanceof Double) {
						if (s instanceof Integer) {
							int comp = compareDouble(s, object);
							if (comp < 0 || comp == 0) {
								value = object;
							}
						}
					}
				} else if (filterType.equals(">=")) {
					if (object instanceof Double) {
						if (s instanceof Integer) {
							int comp = compareDouble(s, object);
							if (comp > 0 || comp == 0) {
								value = object;
							}
						}
					}
				} else if (filterType.equals("!=")) {
					if (object instanceof Double) {
						if (s instanceof Integer) {
							int comp = compareDouble(s, object);
							if (comp < 0 || comp > 0) {
								value = object;
							}
						}
					}
				}
			}

		}
		return value;
	}

	private int compareDouble(Object s, Object object) {
		int comp = 0;
		Double filterDouble = new Double((Integer) s).doubleValue();
		Double valueDouble = (Double) object;
		comp = Double.compare(valueDouble, filterDouble);
		return comp;

	}
}
