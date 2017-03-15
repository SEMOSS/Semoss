package prerna.ds;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.rdf.query.builder.IQueryInterpreter;

public class TinkerBaseGenericIterator extends TinkerBaseIterator{

	public TinkerBaseGenericIterator(GraphTraversal gt, List<String> selectors, QueryStruct qs) {
		this.gt = gt;
		this.selectors = selectors;
		this.filters = qs.andfilters;
		this.performCount = qs.getPerformCount();
		this.rowCount = 0;
		getNextValidRow();
	}

	/**
	 * Sets the nextRow to the valid row or null
	 */
	public void getNextValidRow() {
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
