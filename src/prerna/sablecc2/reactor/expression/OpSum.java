package prerna.sablecc2.reactor.expression;

public class OpSum extends OpBasicMath {

	public OpSum() {
		this.operation = "sum";
	}
	
	@Override
	protected double evaluate(Object[] values) {
		double sum = 0;
		
		for(Object val : values) {
			sum += ((Number) val).doubleValue();
		}
		return sum;
	}
}
