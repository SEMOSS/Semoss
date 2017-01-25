package prerna.ds;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TinkerIterator implements Iterator {
	private GraphTraversal gt;
	private Graph g;
	private List<String> selectors;

	public TinkerIterator(Graph g, GraphTraversal gt, List<String> selectors) {
		this.g = g;
		this.gt = gt;
		this.selectors = selectors;
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
				Map<String, Object> mapData = (Map<String, Object>) data; // cast
																			// to
																			// map
				retObject[colIndex] = ((Vertex) mapData.get(selectors.get(colIndex))).property(TinkerFrame.TINKER_NAME)
						.value();
			}
		} else {
			retObject[0] = ((Vertex) data).property(TinkerFrame.TINKER_NAME).value() + "";
		}

		return retObject;
	}

}
