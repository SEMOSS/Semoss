package prerna.sablecc2.reactor.expression;

public class OpMin extends OpBasicMath {

	public OpMin() {
		this.operation = "min";
	}
	
	@Override
	protected double evaluate(Object[] values) {
		double min = Double.MAX_VALUE;
		
		for(Object val : values) {
			Double nextNumber = ((Number) val).doubleValue();
			min = min > nextNumber ? nextNumber : min;
		}
		
		return min;
	}	
}
