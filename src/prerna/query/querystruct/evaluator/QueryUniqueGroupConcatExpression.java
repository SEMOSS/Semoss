package prerna.query.querystruct.evaluator;

import java.util.LinkedHashSet;
import java.util.Set;

public class QueryUniqueGroupConcatExpression implements IQueryStructExpression {

	private Set<Object> objs = new LinkedHashSet<Object>();
	
	@Override
	public void processData(Object obj) {
		objs.add(obj);
	}
	
	@Override
	public Object getOutput() {
		StringBuilder concat = new StringBuilder();
		boolean first = true;
		for(Object obj : objs) {
			if(first) {
				concat.append(obj);
				first = false;
			} else {
				concat.append(", ").append(obj);
			}
		}
		return concat.toString();
	}

}
