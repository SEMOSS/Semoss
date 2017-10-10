package prerna.query.querystruct.evaluator;

public class QueryStandardDeviationExpression implements IQueryStructExpression {

	/**
	 * Implementation of Welkford's method for calculation of standard deviation
	 */
	
	private double M = 0.0;
	private double S = 0.0;
	int k = 1;
		
	@Override
	public void processData(Object obj) {
		if(obj instanceof Number) {
			double value = ((Number) obj).doubleValue();
			double tempM = this.M;
			this.M += (value - tempM) / this.k;
			this.S += (value - tempM) * (value - this.M);
			this.k++;
		}
	}
	
	@Override
	public Object getOutput() {
		return Math.sqrt(this.S / (k-2) );
	}

}
