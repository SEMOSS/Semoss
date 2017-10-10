package prerna.query.querystruct.evaluator;

import java.util.Set;

import org.antlr.misc.OrderedHashSet;

public class QueryUniqueCountExpression implements IQueryStructExpression {

	private Set<Object> objs = new OrderedHashSet<Object>();
	
	@Override
	public void processData(Object obj) {
		objs.add(obj);
	}
	
	@Override
	public Object getOutput() {
		return this.objs.size();
	}

}
