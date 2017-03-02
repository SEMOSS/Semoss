package prerna.ds;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.rdf.query.builder.IQueryInterpreter;

public class TinkerBaseIterator implements Iterator {
	private GraphTraversal gt;
	private List<String> selectors;
	private Object[] nextRow;
	protected Map<String, Map<String, List>> filters;
	private int performCount;
	private int rowCount;

	public TinkerBaseIterator(GraphTraversal gt, List<String> selectors, QueryStruct qs) {
		this.gt = gt;
		this.selectors = selectors;
		this.filters = qs.andfilters;
		this.performCount = qs.getPerformCount();
		this.rowCount = 0;
		getNextValidRow();

	}

	@Override
	public boolean hasNext() {
		return nextRow != null;
	}

	@Override
	public Object[] next() {
		Object[] row = nextRow;
		getNextValidRow();
		return row;
	}

	/**
	 * Sets the nextRow to the valid row or null
	 */
	private void getNextValidRow() {
		boolean validRow = false;
		while (gt.hasNext() && !validRow) {
			nextRow = getNextRow();
			if (nextRow != null) {
				validRow = true;
				rowCount++;
			}
		}
		if (!validRow) {
			nextRow = null;
		}
	}

	/**
	 * Tinker does not support regular expressions so treat the value as a
	 * string and apply a regex
	 * 
	 * @param value
	 * @param filterInfo
	 * @return
	 */
	public String search(String value, Map<String, List> filterInfo) {
		String match = null;
		for (String filterType : filterInfo.keySet()) {
			if (filterType.equals(IQueryInterpreter.SEARCH_COMPARATOR)) {
				List filterVals = filterInfo.get(filterType);
				String strPattern = (String) filterVals.get(0);
				String pattern = ".*" + strPattern.toLowerCase() + ".*";
				Pattern r = Pattern.compile(pattern);

				Matcher m = r.matcher(value.toLowerCase());
				if (m.find()) {
					match = value;
				}
			}
			else {
				match = value;
			}
		}
		return match;
	}

	private Object[] getNextRow() {
		Object data = gt.next();
		Object[] retObject = new Object[selectors.size()];

		// iterate through selectors and check if the selector is a node or node
		// property

		// data will be a map for multi columns
		if (data instanceof Map) {
			for (int colIndex = 0; colIndex < selectors.size(); colIndex++) {
				Map<String, Object> mapData = (Map<String, Object>) data;
				String select = selectors.get(colIndex);
				Object vertOrProp = mapData.get(select);
				Object value = null;
				if (vertOrProp instanceof Vertex) {
					value = ((Vertex) vertOrProp).property(TinkerFrame.TINKER_NAME).value();
				} else {
					value = vertOrProp;
				}
				retObject[colIndex] = value;
			}
		} else {

			for (int colIndex = 0; colIndex < selectors.size(); colIndex++) {
				String select = selectors.get(colIndex);
				Object value = data;
				boolean filtered = false;
				if (data instanceof Vertex) {
					value = ((Vertex) data).values(TinkerFrame.TINKER_NAME).next();
					System.out.println(value);
					if (filters.containsKey(select)) {
						value = search(value + "", filters.get(select));
						filtered = true;
					}
				}

				else {
					value = data;
				}
				retObject[colIndex] = value;
				if (filtered) {
					if (value == null) {
						retObject = null;
					}
				}
			}
		}

		return retObject;
	}

}
