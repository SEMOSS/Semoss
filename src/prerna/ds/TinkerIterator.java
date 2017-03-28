package prerna.ds;

import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

public class TinkerIterator implements Iterator {
	private TinkerBaseIterator it;
	private String[] headers;

	public TinkerIterator(GraphTraversal gt, List<String> selectors, QueryStruct qs) {

		if (selectors.contains("GROUP_BY")) {
			// create group by iterator
			it = new TinkerBaseGroupByIterator(gt, selectors, qs);
		} else {
			it = new TinkerBaseGenericIterator(gt, selectors, qs);
		}

		// create Headers
		headers = new String[selectors.size()];
		for (int i = 0; i < selectors.size(); i++) {
			headers[i] = selectors.get(i);

		}
	}

	public String[] getHeaders() {
		return headers;
	}

	@Override
	public boolean hasNext() {
		return it.hasNext();
	}

	@Override
	public Object[] next() {
		return it.next();
	}

}
