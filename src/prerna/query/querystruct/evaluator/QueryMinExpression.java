package prerna.query.querystruct.evaluator;

public class QueryMinExpression implements IQueryStructExpression {

	private double minValue = Double.MAX_VALUE;
	
	@Override
	public void processData(Object obj) {
		if(obj instanceof Number) {
			double testValue = ((Number) obj).doubleValue();
			if(testValue < this.minValue) {
				this.minValue = testValue;
			}
		}
	}
	
	@Override
	public Object getOutput() {
		return this.minValue;
	}

}
