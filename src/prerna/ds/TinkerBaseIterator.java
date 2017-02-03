package prerna.ds;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TinkerBaseIterator implements Iterator {
	private GraphTraversal gt;
	private List<String> selectors;
	private Object[] nextRow;

	public TinkerBaseIterator(GraphTraversal gt, List<String> selectors) {
		this.gt = gt;
		this.selectors = selectors;
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
				if (data instanceof Vertex) {
					value = ((Vertex) data).values(TinkerFrame.TINKER_NAME).next();
				}

				else {
					value = data;
				}
				retObject[colIndex] = value;
			}
		}

		return retObject;
	}

}
