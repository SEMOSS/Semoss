package prerna.query.querystruct.evaluator;

public class QueryMaxExpression implements IQueryStructExpression {

	private double maxValue = -1 * Double.MAX_VALUE;
	
	@Override
	public void processData(Object obj) {
		if(obj instanceof Number) {
			double testValue = ((Number) obj).doubleValue();
			if(testValue > this.maxValue) {
				this.maxValue = testValue;
			}
		}
	}
	
	@Override
	public Object getOutput() {
		return this.maxValue;
	}

}
