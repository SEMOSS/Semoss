package prerna.query.querystruct.evaluator;

public class QueryAverageExpression implements IQueryStructExpression {

	private double sum = 0.0;
	private int count = 0;
	
	@Override
	public void processData(Object obj) {
		if(obj instanceof Number) {
			double newValue = ((Number) obj).doubleValue();
			this.sum += newValue;
			this.count++;
		}
	}
	
	@Override
	public Object getOutput() {
		if(this.count == 0) {
			return 0;
		}
		return this.sum / this.count;
	}

}
