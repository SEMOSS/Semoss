package prerna.sablecc2.reactor.expression;

public class OpMax extends OpBasicMath {	
	
	public OpMax() {
		this.operation = "max";
	}
	
	@Override
	protected double evaluate(Object[] values) {
		double max = -1.0 * Double.MAX_VALUE;
		
		for(Object val : values) {
			Double nextDouble = ((Number) val).doubleValue();
			max = max < nextDouble ? nextDouble : max;
		}
		return max;
	}	
}
