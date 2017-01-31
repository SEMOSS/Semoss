package prerna.ds;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

public class TinkerIterator implements Iterator {
	private TinkerBaseIterator it;

	public TinkerIterator(GraphTraversal gt, List<String> selectors, HashMap<String, String> propHash,
			Vector<String> nodeSelector, Hashtable<String, Hashtable<String, Vector>> filters) {
		it = new TinkerBaseIterator(gt, selectors, propHash, nodeSelector, filters);
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
