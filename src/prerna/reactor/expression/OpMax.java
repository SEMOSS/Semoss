package prerna.reactor.expression;

public class OpMax extends OpBasicMath {	
	
	public OpMax() {
		this.operation = "max";
	}
	
	@Override
	protected double evaluate(Object[] values) {
		return eval(values);
	}	
	
	public static double eval(Object...values) {
		double max = -1.0 * Double.MAX_VALUE;
		
		for(Object val : values) {
			Double nextDouble = ((Number) val).doubleValue();
			max = max < nextDouble ? nextDouble : max;
		}
		return max;
	}
	
	public static double eval(double[] values) {
		double max = -1.0 * Double.MAX_VALUE;
		
		for(Object val : values) {
			Double nextDouble = ((Number) val).doubleValue();
			max = max < nextDouble ? nextDouble : max;
		}
		return max;
	}
}
