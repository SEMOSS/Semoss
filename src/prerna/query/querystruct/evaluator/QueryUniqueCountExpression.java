package prerna.query.querystruct.evaluator;

import java.util.LinkedHashSet;
import java.util.Set;

public class QueryUniqueCountExpression implements IQueryStructExpression {

	private Set<Object> objs = new LinkedHashSet<Object>();
	
	@Override
	public void processData(Object obj) {
		objs.add(obj);
	}
	
	@Override
	public Object getOutput() {
		return this.objs.size();
	}

}
