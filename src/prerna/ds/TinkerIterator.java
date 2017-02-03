package prerna.ds;


import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

public class TinkerIterator implements Iterator {
	private TinkerBaseIterator it;

	public TinkerIterator(GraphTraversal gt, List<String> selectors) {
		it = new TinkerBaseIterator(gt, selectors);
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
