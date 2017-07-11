package prerna.query.interpreters;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Graph;

public class GremlinInterpreter2 extends AbstractQueryInterpreter {

	protected Graph g;
	
	public GremlinInterpreter2(Graph g) {
		this.g = g;
	}
	
	public Iterator composeIterator() {
		
		return null;
	}
	
	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String composeQuery() {
		return null;
	}
}
