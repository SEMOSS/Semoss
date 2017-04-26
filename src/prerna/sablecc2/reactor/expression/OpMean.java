package prerna.sablecc2.reactor.expression;

public class OpMean extends OpBasicMath {

	public OpMean() {
		this.operation = "avg";
	}
	
	@Override
	protected double evaluate(Object[] values) {
		double sum = 0;
		for(Object val : values) {
			sum += ((Number) val).doubleValue();
		}
		double mean = sum/values.length;

		return mean;
	}
}