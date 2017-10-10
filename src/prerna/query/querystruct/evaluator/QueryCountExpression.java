package prerna.query.querystruct.evaluator;

public class QueryCountExpression implements IQueryStructExpression {

	private int count = 0;
	
	@Override
	public void processData(Object obj) {
		this.count++;
	}
	
	@Override
	public Object getOutput() {
		return this.count;
	}

}
