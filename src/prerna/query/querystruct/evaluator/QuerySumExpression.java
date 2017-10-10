package prerna.query.querystruct.evaluator;

public class QuerySumExpression implements IQueryStructExpression {

	private double sum = 0.0;
	
	@Override
	public void processData(Object obj) {
		if(obj instanceof Number) {
			this.sum += ((Number) obj).doubleValue();
		}
	}
	
	@Override
	public Object getOutput() {
		return this.sum;
	}

}
